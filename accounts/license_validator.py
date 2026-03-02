import os
import hashlib
import requests # NEW
from django.conf import settings
from .hardware_id import get_hardware_id

LICENSE_FILE = os.path.join(settings.BASE_DIR, 'license.key')
SECRET_SALT = "Auditcore_v1_Secret_Salt_2026"

# Replace with your actual Firebase Realtime Database URL
FIREBASE_URL = "https://auditcore-55139-default-rtdb.asia-southeast1.firebasedatabase.app/allowed_hardware"

def generate_license_key(hardware_id):
    raw_string = f"{hardware_id}{SECRET_SALT}"
    signature = hashlib.sha256(raw_string.encode()).hexdigest()
    return f"{signature}|{hardware_id}"

def check_firebase_status(hwid):
    """
    Returns:
      True  -> Admin explicitly set it to true
      False -> Admin explicitly set it to false (REVOKED)
      None  -> Offline or Firebase unreachable
    """
    try:
        url = f"{FIREBASE_URL}/{hwid}.json"
        response = requests.get(url, timeout=3)
        
        if response.status_code == 200:
            data = response.json()
            if data is True:
                return True
            elif data is False:
                return False
    except Exception:
        pass # Offline or network timeout
        
    return None

def is_license_valid():
    """Checks if the local license.key file exists and matches THIS machine."""
    if not os.path.exists(LICENSE_FILE):
        return False, "No license file found."

    try:
        with open(LICENSE_FILE, 'r') as f:
            stored_key = f.read().strip()
            
        stored_signature, stored_hwid = stored_key.split('|')
        current_hwid = get_hardware_id()
        
        if stored_hwid != current_hwid:
            return False, "License is bound to a different machine."
            
        expected_key = generate_license_key(current_hwid)
        if stored_key == expected_key:
            return True, "Valid"
            
        return False, "License file is corrupted."
    except Exception:
        return False, "Invalid license format."

def activate_license(input_key):
    """Writes the license key to disk."""
    current_hwid = get_hardware_id()
    expected_key = generate_license_key(current_hwid)
    if input_key == expected_key:
        with open(LICENSE_FILE, 'w') as f:
            f.write(input_key)
        return True
    return False

def deactivate_license():
    """Deletes the local license key."""
    if os.path.exists(LICENSE_FILE):
        os.remove(LICENSE_FILE)
        return True
    return False