import asyncio
from dataclasses import dataclass
from datetime import timezone, datetime
import json
import logging

from meshcore import MeshCore, EventType

log = logging.getLogger('contact struct')
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

def _advert_to_contactinfo(advert) -> ContactInfo:
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

async def main():
    mc = await MeshCore.create_serial('/dev/ttyACM0', 115200)
    result = await mc.commands.get_contacts()
    if result.type == EventType.ERROR:
        print(f"Foo: {result.payload}")
    contacts = result.payload

    for pubkey, advert in contacts.items():
        contact = _advert_to_contactinfo(advert)
        print(f"{pubkey}: {contact}")

if __name__ == '__main__':
    asyncio.run(main())
