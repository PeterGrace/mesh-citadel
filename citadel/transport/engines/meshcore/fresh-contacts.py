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
if the contact's pubkey doesn't exist in the node, will remove the
oldest contact on the node and add the new one.  Optionally, if the
MeshCore library offers this functionality, the contact's details will
be updated rather than the remove/re-add process.

# Delete contact

Deleting a contact will remove it from the node and from the database.
This is unlikely to be used much in production, but makes sense to
offer as part of an expected set of functions for a manager like this.

# Expire contact

Expiring a contact will not take an argument like a pubkey or name
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


class ContactManager:
    def __init__(self, meshcore, db, config):
        self.db = db
        self.meshcore = meshcore
        self.config = config.transport.get("meshcore",
                                           {}).get("contact_manager", {})
        # node_id -> (name, pubkey)
        # cache should only contain contacts which are available in node
        # memory
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

        await self._load_db_cache()
        await self._load_node_cache()
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
        node_contacts = await self._count_node_contacts()
        db_contacts = await self._count_db_contacts()

        if node_contacts <= db_contacts:
            log.info("Synchronizing database contacts to node")
            await self._sync_db_to_node()
        elif db_contacts < node_contacts:
            log.info("Synchronizing node contacts to database")
            await self._sync_node_to_db()
            await self._load_db_cache()

    #------------------------------------------------------------
    # Public CRUD methods
    #------------------------------------------------------------

    async def add_contact(self, contact: ContactInfo):
        # add the contact to the database and the node, making space on
        # the node if necessary.  business logic only here, calling
        # helpers.  if possible, edit an existing node contact,
        # otherwise remove and re-add
        node_contacts = await self._count_node_contacts()

        try:
            if node_contacts >= self.max_node_contacts:
                await self._expire_single_contact()
            await self._add_contact_to_db(contact)
            await self._add_contact_to_node(contact)
            self._add_contact_to_cache(contact)
        except Exception as err:
            log.exception(f"Unable to add {contact.name}: {err}")

    async def delete_contact(self, contact: ContactInfo):
        # remove the contact from both database and node
        pass

    async def expire_contact(self):
        # remove the oldest contact from the node's contact list
        pass

    async def get_contact(self, node_id: str, from_node=False) -> ContactInfo:
        # retrieve the contact from the database, or optionally from
        # the node memory (which takes much longer).  node_id needs at
        # least the first 16 characters of the pubkey, but can also be
        # a full pubkey.
        pass

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
                node_type=int(advert['node_type']),
                name=advert['adv_name'],
                latitude=float(advert.get("adv_lat", 0.0)),
                longitude=float(advert.get("adv_lon", 0.0)),
                raw_advert_data=str_advert,
            )
        except Exception as err:
            log.exception(f"Unable to convert {advert} to ContactInfo: {err}")
            return
        return contact


    #------------------------------------------------------------
    # Synchronization helpers
    #------------------------------------------------------------

    async def _sync_db_to_node(self):
        """Synchronize the contacts which are currently in the
        database, up to the node's memory limit, into the node's
        contact list."""
        pass

    async def _sync_node_to_db(self):
        """Synchronize the contacts which are currently on the node
        into the database. Usually, this will occur when the BBS is
        newly installed, but the node already has some contacts
        available."""
        pass

    #------------------------------------------------------------
    # Node/MeshCore helpers
    #------------------------------------------------------------

    async def _load_node_cache(self):
        """Reload the node cache with the current contends of the node's
        contact list"""
        all_contacts = await self._get_node_contacts()
        self._node_cache = {}
        for node_id, contact in all_contacts.items():
            self._node_cache[node_id] = (contact.name, contact.public_key)

    async def _get_node_contacts(self):
        """Return a dict of all node contacts, of the form {node_id:
        contact_info}"""
        node_contacts = await self.meshcore.commands.get_contacts()
        parsed_contacts = {}
        for pubkey, data in node_contacts.items():
            contact = self._advert_to_contactinfo(data)
            parsed_contacts[pubkey[:16]] = contact
        return parsed_contacts

    async def _count_node_contacts(self):
        """Get a count of the number of contacts currently in the
        node's contact list. This is a relatively time-consuming
        call."""
        node_contacts = await self.meshcore.commands.get_contacts()
        return len(node_contacts)

    async def _add_contact_to_node(self, contact: ContactInfo):
        """Blindly add the given contact to the node.  This function
        *does not* handle making space for a new contact."""
        pass

    async def _delete_contact_from_node(self, pubkey):
        """Blindly delete the given contact from the node.  This
        function is only intended to operate on a specific contact."""
        pass

    async def _expire_single_contact(self):
        """Find the oldest contact on the node, and remove it."""
        pass

    #------------------------------------------------------------
    # Database helpers
    #------------------------------------------------------------

    async def _load_db_cache()
        """Reload the cache with the current contents of the
        database"""
        query = "SELECT node_id, name, public_key FROM mc_chat_contacts"
        contacts = await self.db.execute(query)

        self._cache = {}
        for row in contacts:
            node_id, name, pubkey = row
            self._db_cache[node_id] = (name, pubkey)

    async def _count_db_contacts(self):
        """Get a count of the number of contacts stored in the
        database."""
        query = "SELECT COUNT(*) FROM mc_chat_contacts"
        count = await self.db.execute(query)
        if count:
            return count[0][0]
        return 0
