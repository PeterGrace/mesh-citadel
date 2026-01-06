"""This module presents the ContactManager class, which performs the
following public functions:

    * Contact sync between node and DB
    * Add/remove/retrieve contact info

Internally, it exercises the following logic:

# Contact sync

Calling the sync method causes the database and the node contact lists
to become synchronized.  Specifically, the node will always be limited
to the number of contacts allowed by the config.yaml file, while the
database will not be size-limited.

In cases where the node has more contacts than the database, the node's
contact list will add to the database's list.  When the database has
more contacts than the node, database contacts will be written to the node
until its memory is at capacity.

Although the node is the source of truth as far as which contacts may
be used for communication, the database will usually hold a more
complete list due to memory size constraints on the node.

# Add contact

Adding a contact will add or update a contact in the database.  It will
also search for, remove, and re-add an existing contact on the node, or
if the contact's public_key doesn't exist in the node, will remove the
oldest contact on the node and add the new one.  Optionally, if the
MeshCore library offers this functionality, the contact's details will
be updated rather than the remove/re-add process.

# Delete contact

Deleting a contact will remove it from the node and from the database.
This is unlikely to be used much in production, but makes sense to
offer as part of an expected set of functions for a manager like this.

# Expire contact

Expiring a contact will not take an argument like a public_key or name
(that's what the remove contact method is for), it will simply go into
the list of node contacts and remove the oldest one, usually in order
to make space.  Expiring a contact does NOT remove it from the
database.

# Retrieve contact

This will retrieve information from the database (or optionally from
the node, if the correct flag is passed), and return it in ContactInfo
format.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging

from meshcore import EventType

log = logging.getLogger(__name__)
UTC = timezone.utc

@dataclass
class ContactInfo:
    node_id: str
    public_key: str
    name: str = ""
    node_type: int = 1
    latitude: float = 0.0
    longitude: float = 0.0
    first_seen: datetime = None
    last_seen: datetime = None
    raw_advert_data: str = ""

    def __post_init__(self):
        if not self.first_seen:
            self.first_seen = datetime.now(UTC)
        if not self.last_seen:
            self.last_seen = datetime.now(UTC)


@dataclass
class CacheEntry:
    name: str
    public_key: str
    last_seen: datetime


class ContactManager:
    def __init__(self, meshcore, db, config):
        self.db = db
        self.meshcore = meshcore
        self.config = config.transport.get("meshcore",
                                           {}).get("contact_manager", {})
        # node_id -> CacheEntry
        self._db_cache = {}
        self._node_cache = {}
        self._running = False

    async def start(self):
        """Start the ContactManager service. This includes performing a
        synchronization, if the 'update_contacts' field is set to true
        in config.yaml."""
        if self._running:
            return

        if self.meshcore:
            result = await self.meshcore.commands.set_manual_add_contacts(True)
            if result.type == EventType.ERROR:
                log.warning(f"Unable to disable auto-add of contacts: {result.payload}")
            else:
                log.info("Disabled auto-add of contacts on node")

        if self.config.get("update_contacts", False):
            self.synchronize()

        log.info("Initializing contact caches")
        await self._load_db_cache()
        await self._load_node_cache()
        log.info("ContactManager startup complete")
        self._running = True

    #------------------------------------------------------------
    # Properties
    #------------------------------------------------------------

    @property
    def max_node_contacts(self):
        max_contacts = self.config.get("max_device_contacts", 100)
        buffer = self.config.get("contact_limit_buffer", 0)
        return max_contacts - buffer

    #------------------------------------------------------------
    # Public sync methods
    #------------------------------------------------------------

    async def synchronize(self):
        """Copy contact list from node to db (if the node has more
        contacts) or from db to node (if the db has more contacts).
        Does not touch caches, which must be updated separately."""
        node_contacts = await self._count_node_contacts()
        db_contacts = await self._count_db_contacts()

        if node_contacts <= db_contacts:
            log.info("Synchronizing database contacts to node")
            await self._sync_db_to_node()
        elif db_contacts < node_contacts:
            log.info("Synchronizing node contacts to database")
            await self._sync_node_to_db()

    #------------------------------------------------------------
    # Public CRUD methods
    #------------------------------------------------------------

    async def add_contact(self, contact: ContactInfo):
        """Add the specified contact to the database and the node's contact
        list."""
        node_contacts = await self._count_node_contacts()

        try:
            if node_contacts >= self.max_node_contacts:
                await self.expire_contact()
            await self._add_contact_to_db(contact)
            await self._add_contact_to_node(contact)
        except Exception as err:
            log.exception(f"Unable to add {contact.name}: {err}")

    async def delete_contact(self, contact: ContactInfo):
        """Remove the named contact from both node and database contact
        lists"""
        await self._delete_contact_from_node(contact)
        await self._delete_contact_from_db(contact)
        log.info(f"Deleted {contact.name} ({contact.node_id}) from db and node")

    async def expire_contact(self):
        """Remove the oldest contact from the node's contact list,
        without modifying the contents of the database's contact
        list."""
        oldest = 0
        oldest_public_key = ""
        for entry in self._node_cache.values():
            if oldest == 0 or entry.last_seen < oldest:
                oldest = entry.last_seen
                oldest_public_key = entry.public_key
        if oldest_public_key:
            await self._delete_contact_from_node(oldest_public_key)

    async def get_contact(self, node_id: str, from_node=False) -> ContactInfo:
        # retrieve the contact from the database, or optionally from
        # the node memory (which takes much longer).  node_id needs at
        # least the first 16 characters of the public_key, but can also be
        # a full public_key.
        # TODO: this is required for the event handler to work
        if from_node:
            contact = await self._get_node_contact(node_id)
        else:
            contact = await self._get_db_contact(node_id)
        return contact


    #------------------------------------------------------------
    # Advert handler
    #------------------------------------------------------------

    async def handle_advert(self, event):
        """Upon reception of a new advert from MeshCore, add the new
        contact to our system if it's a companion node."""
        try:
            advert_data = event.payload
            public_key = advert_data.get('public_key', '')
            if not public_key:
                log.warning("Advert missing public key")
                return

            node_id = public_key[:16]

            if event.type == EventType.NEW_CONTACT:
                contact = self._advert_to_contactinfo(advert_data)
            else:
                contact = await self.get_contact(node_id)
            if not contact:
                log.warning(f"Unable to add {node_id}: couldn't get contact details")
                return
            
            if not self._is_companion(contact): 
                log.debug(f"Discarding non-companion advert: {contact}")
                return

            await self.add_contact(contact)
            log.info(f"Recorded advert: {contact.name} ({node_id})")
        except Exception as err:
            log.exception(f"Unable to process new advert: {err}")

    #------------------------------------------------------------
    # Contact helpers
    #------------------------------------------------------------

    def _advert_to_contactinfo(self, advert) -> ContactInfo:
        """Convert the data from an advert into a ContactInfo object. Takes
        either a JSON string or a dict as advert."""
        if isinstance(advert, str):
            try:
                str_advert = advert
                advert = json.loads(advert)
            except json.JSONDecodeError as err:
                log.exception(f"Unable to convert {str_advert} to dict: {err}")
                return
        else:
            str_advert = json.dumps(advert)

        try:
            contact = ContactInfo(
                node_id=advert['public_key'][:16],
                public_key=advert['public_key'],
                node_type=int(advert['type']),
                name=advert['adv_name'],
                latitude=float(advert.get("adv_lat", 0.0)),
                longitude=float(advert.get("adv_lon", 0.0)),
                raw_advert_data=str_advert,
            )
        except Exception as err:
            log.exception(f"Unable to convert {advert} to ContactInfo: {err}")
            return
        return contact

    def _contactinfo_to_advert(self, contact) -> dict:
        """Convert a ContactInfo struct into an advert dictionary"""
        advert = {}
        advert['public_key'] = contact.public_key
        advert['type'] = int(contact.node_type)
        advert['flags'] = 0
        advert['out_path_len'] = 0
        advert['out_path'] = ''
        advert['adv_name'] = contact.name
        if isinstance(contact.last_seen, str):
            contact.last_seen = datetime.fromisoformat(contact.last_seen)
        advert['last_advert'] = int(contact.last_seen.strftime('%s'))
        advert['adv_lat'] = contact.latitude
        advert['adv_lon'] = contact.longitude
        advert['lastmod'] = advert['last_advert']
        return advert

    def _is_companion(self, contact) -> bool:
        return contact.node_type == 1

    def _make_cache_entry(self, contact) -> CacheEntry:
        return CacheEntry(
            name=contact.name,
            public_key=contact.public_key,
            last_seen=contact.last_seen
        )


    #------------------------------------------------------------
    # Synchronization helpers
    #------------------------------------------------------------

    async def _sync_db_to_node(self):
        """Synchronize the contacts which are currently in the
        database, up to the node's memory limit, into the node's
        contact list."""
        capacity = self.max_node_contacts
        query = """
            SELECT node_id, raw_advert_data
            FROM mc_chat_contacts
            ORDERED BY last_seen DESC
        """
        data = await self.db.execute(query)
        count = 0
        for node_id, raw_advert_data in data:
            if count <= capacity:
                contact = self._advert_to_contactinfo(raw_advert_data)
                result = await self._add_contact_to_node(contact)
                if result:
                    count += 1
        log.info(f"Synced {count} contacts from database to node")

    async def _sync_node_to_db(self):
        """Synchronize the contacts which are currently on the node
        into the database. Usually, this will occur when the BBS is
        newly installed, but the node already has some contacts
        available."""
        contacts = self._get_all_node_contacts
        count = 0
        for node_id, contact in contacts.items():
            await self._add_contact_to_db(contact)
            count += 1
        log.info(f"Synced {count} contacts from node to database")

    #------------------------------------------------------------
    # Node/MeshCore helpers
    #------------------------------------------------------------

    async def _load_node_cache(self):
        """Reload the node cache with the current contends of the node's
        contact list"""
        all_contacts = await self._get_all_node_contacts()
        self._node_cache = {}
        for node_id, contact in all_contacts.items():
            self._node_cache[node_id] = self._make_cache_entry(contact)

    async def _get_all_node_contacts(self):
        """Return a dict of all node contacts, of the form {node_id:
        contact_info}"""
        node_contacts = await self.meshcore.commands.get_contacts()
        parsed_contacts = {}
        for public_key, data in node_contacts.payload.items():
            contact = self._advert_to_contactinfo(data)
            parsed_contacts[public_key[:16]] = contact
        return parsed_contacts

    async def _get_node_contact(self, node_id: str) -> ContactInfo:
        """Retrieve a specified contact from the node's contact list"""
        result = await self.meshcore.get_contact_by_key_prefix(node_id)
        if result.type == EventType.ERROR:
            log.error(f"Unable to get {node_id} from node: {result.payload}")
            return
        contact = self._advert_to_contactinfo(result.payload)
        return contact

    async def _count_node_contacts(self) -> int:
        """Get a count of the number of contacts currently in the
        node's contact list. This is a relatively time-consuming
        call."""
        result = await self.meshcore.commands.get_contacts()
        if result.type == EventType.ERROR:
            log.error(f"Unable to count node contacts: {result.payload}")
        return len(result.payload)

    async def _add_contact_to_node(self, contact: ContactInfo) -> bool:
        """Blindly add the given contact to the node, and the node
        cache.  This function *does not* handle making space for a new
        contact."""
        if contact.raw_advert_data:
            try:
                advert = json.loads(contact.raw_advert_data)
            except (json.JSONDecodeError, TypeError):
                advert = self._contactinfo_to_advert(contact)
            result = await self.meshcore.commands.add_contact(advert)
        else:
            log.error(f"Unable to add {contact.name} to node contact list: missing advert string")
            return False
        if result.type == EventType.ERROR:
            log.error(f"Unable to add {contact.name} to node contact list: {result.payload}")
            return False
        self._node_cache[contact.node_id] = self._make_cache_entry(contact)
        return True

    async def _delete_contact_from_node(self, identifier):
        """Blindly delete the given contact from the node.  This
        function is only intended to operate on a specific contact."""
        if isinstance(identifier, ContactInfo):
            public_key = identifier.public_key
        elif isinstance(identifier, str):
            public_key = identifier
        else:
            log.error(f"Malformed data passed to _delete_contact_from_node: {identifier}")
            return False

        result = await self.meshcore.commands.remove_contact(public_key)
        if result.type == EventType.ERROR:
            log.error(f"Unable to remove {public_key} from node: {result.payload}")
            return False
        return True

    #------------------------------------------------------------
    # Database helpers
    #------------------------------------------------------------

    async def _load_db_cache(self):
        """Reload the cache with the current contents of the
        database"""
        query = "SELECT node_id, name, public_key, last_seen FROM mc_chat_contacts"
        contacts = await self.db.execute(query)

        self._cache = {}
        for row in contacts:
            node_id, name, public_key, last_seen = row
            contact = ContactInfo(node_id=node_id,
                                  public_key=public_key, name=name,
                                  last_seen=last_seen)
            self._db_cache[node_id] = self._make_cache_entry(contact)

    async def _get_db_contact(self, node_id: str) -> ContactInfo:
        """Retrieve a single contact record from the database."""
        query = "SELECT * FROM mc_chat_contacts WHERE node_id = ?"
        try:
            result = await self.db.execute(query, (node_id,))
        except RuntimeError as err:
            log.error(f"Unable to get {node_id} from database: {err}")
            return
        if not result or not isinstance(result, list):
            log.error(f"Unable to get {node_id} from database: No data")
            return

        data = result[0]
        contact = ContactInfo(
            node_id=data[0],
            public_key=data[1],
            name=data[2],
            node_type=data[3],
            latitude=data[4],
            longitude=data[5],
            first_seen=data[6],
            last_seen=data[7],
            raw_advert_data=data[8],
        )
        return contact

    async def _count_db_contacts(self):
        """Get a count of the number of contacts stored in the
        database."""
        query = "SELECT COUNT(*) FROM mc_chat_contacts"
        count = await self.db.execute(query)
        if count:
            return count[0][0]
        return 0

    async def _add_contact_to_db(self, contact: ContactInfo):
        """Add contact to the database contact list, updating the cache
        in the process."""
        query = """
            INSERT INTO mc_chat_contacts (
                node_id,  
                public_key,
                name,
                node_type,
                latitude,             
                longitude,
                first_seen,
                last_seen,
                raw_advert_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET   
                public_key = excluded.public_key,
                name = excluded.name,
                node_type = excluded.node_type,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                last_seen = excluded.last_seen,
                raw_advert_data = excluded.raw_advert_data
        """
        try:
            await self.db.execute(query, (
                contact.node_id,
                contact.public_key,
                contact.name,
                contact.node_type,
                contact.latitude,
                contact.longitude,
                contact.first_seen,
                contact.last_seen,
                contact.raw_advert_data
                )
            )
        except RuntimeError as err:
            log.exception(f"Unable to add contact to database: {err}")
            return
        self._db_cache[contact.node_id] = self._make_cache_entry(contact)

    async def _delete_contact_from_db(self, contact: ContactInfo):
        """Remove the specified contact from the database"""
        query = "DELETE FROM mc_chat_contacts WHERE node_id = ?"
        try:
            await self.db.execute(query, contact.node_id)
        except RuntimeError as err:
            log.error("Unable to delete contact from database: {contact}")
            log.exception(err)
        del self._db_cache[node_id]
