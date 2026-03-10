from faker import Faker
from faker.providers import BaseProvider
import random

class SupplyChainProvider(BaseProvider):
    def product_name(self):
        products = [
            "Olive Oil",
            "Extra Virgin Olive Oil",
            "Refined Olive Oil",
            "Corn Oil",
            "Soy Oil",
            "Wheat Flour",
            "Coffee Beans",
            "Roasted Coffee",
            "Organic Olives",
            "Table Olives",
        ]
        return random.choice(products)

    def product_sku(self):
        return f"{self._letters(3)}-{self._digits(5)}-{self._letters(1)}"

    def gtin14(self):
        digits = [random.randint(0,9) for _ in range(13)]
        total = sum((3 if (i % 2)==0 else 1) * d for i, d in enumerate(reversed(digits)))
        check = (10 - (total % 10)) % 10
        return "".join(map(str, digits+[check]))

    def batch_id(self):
        return f"B{self._digits(8)}"

    def company_name_supplier(self, prefixes=None, suffixes=None):
        prefixes = prefixes or ["Global","Prime","Atlas","Vertex","Nova","Apex"]
        suffixes = suffixes or ["Metals","Plastics","Chemicals","Components","Alloys","Commodities","Cooperative"]
        return f"{random.choice(prefixes)} {random.choice(suffixes)} Ltd"

    def manufacturer_name(self):
        return f"{self.generator.company()} Manufacturing"

    def distributor_name(self):
        return f"{self.generator.company()} Distribution"

    def certification_body_name(self):
        return f"{self.generator.company()} Certification"

    def regulator_name(self):
        return f"{self.generator.city()} Food Authority"

    def grower_name(self):
        last = self.generator.last_name()
        suffix = random.choice(["Groves", "Orchards", "Farms", "Estate", "Agri"])
        return f"{last} {suffix}"

    def retailer_name(self):
        city = self.generator.city()
        noun = random.choice(["Market", "Foods", "Provisions", "Grocers", "Supermart"])
        return f"{city} {noun}"

    def farm_name(self):
        last = self.generator.last_name()
        descriptor = random.choice(["Farm", "Estate", "Ranch", "Orchard"])
        return f"{last} {descriptor}"

    def mill_name(self):
        city = self.generator.city()
        suffix = random.choice(["Mill", "Crush Station", "Press House", "Olive Mill"])
        return f"{city} {suffix}"

    def facility_name(self):
        city = self.generator.city()
        suffix = random.choice(["Hub", "Warehouse", "Center", "Depot", "Plant", "Facility", "Site"])
        return f"{city} {suffix}"

    def processing_plant_name(self):
        return f"{self.generator.city()} Processing Plant"

    def quality_lab_name(self):
        return f"{self.generator.city()} Quality Lab"

    def retail_store_name(self):
        city = self.generator.city()
        noun = random.choice(["Store", "Market", "Shop", "Outlet"])
        return f"{city} {noun}"

    def port_name(self):
        return f"{self.generator.city()} Port"

    def transport_vehicle_name(self):
        return f"Truck {self._letters(2)}-{self._digits(4)}"

    def shipment_code(self):
        return f"SHIP-{self._digits(5)}"

    def harvest_batch_label(self):
        prefix = random.choice(["Lot", "Batch", "Harvest"])
        return f"{prefix} {self._letters(2)}-{self._digits(4)}"

    def material_lot_label(self):
        prefix = random.choice(["MaterialLot", "Lot", "ML"])
        return f"{prefix} {self._letters(2)}-{self._digits(4)}"

    def product_batch_label(self):
        prefix = random.choice(["ProductBatch", "Batch", "PB"])
        return f"{prefix} {self._letters(2)}-{self._digits(4)}"

    def logistics_provider_name(self):
        carriers = [
            "FedEx",
            "UPS",
            "DHL",
            "Maersk",
            "Geodis",
            "C.H. Robinson",
            "DB Schenker",
            "CEVA",
        ]
        return random.choice(carriers)

    def warehouse_name(self):
        city = self.generator.city()
        suffix = random.choice(["Warehouse", "Fulfillment Center", "Distribution Hub"])
        return f"{city} {suffix}"

    def distribution_center_name(self):
        city = self.generator.city()
        suffix = random.choice(["DC", "Distribution Center", "Regional Hub"])
        return f"{city} {suffix}"

    # helpers
    def _letters(self, n):
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        return "".join(random.choice(alphabet) for _ in range(n))

    def _digits(self, n):
        return "".join(str(random.randint(0,9)) for _ in range(n))

def make_faker(locale="en"):
    f = Faker(locale)
    f.add_provider(SupplyChainProvider)
    return f
