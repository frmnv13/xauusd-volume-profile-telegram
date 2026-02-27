import math
from config import PRICE_STEP

class VolumeProfileEngine:
    def __init__(self):
        """
        Inisialisasi dictionary untuk menyimpan volume profile.
        Key: Harga (float, dibulatkan 2 desimal)
        Value: Akumulasi Volume (float)
        """
        self.volume_profile = {}
        self.total_volume = 0.0

    def process_candle(self, high_p, low_p, volume):
        """
        Memecah volume candle M1 menjadi irisan harga (slicing) per PRICE_STEP.
        Menggunakan logika distribusi rata (flat) dari Low ke High.
        """
        self.total_volume += volume
        
        start_bin = round(math.floor(low_p / PRICE_STEP) * PRICE_STEP, 2)
        end_bin = round(math.floor(high_p / PRICE_STEP) * PRICE_STEP, 2)

        num_bins = int(round((end_bin - start_bin) / PRICE_STEP)) + 1
        
        vol_per_bin = volume / num_bins

        for i in range(num_bins):
            price_key = round(start_bin + (i * PRICE_STEP), 2)
            self.volume_profile[price_key] = self.volume_profile.get(price_key, 0.0) + vol_per_bin

    def get_poc(self):
        """
        Mengembalikan harga (Price of Control) dengan volume tertinggi dan total volume.
        """
        if not self.volume_profile:
            return 0.0, 0.0
        
        poc = max(self.volume_profile, key=self.volume_profile.get)
        return poc, self.total_volume

    def reset(self):
        """Reset engine untuk sesi baru."""
        self.volume_profile.clear()
        self.total_volume = 0.0