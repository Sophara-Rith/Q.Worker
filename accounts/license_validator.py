import os
import hashlib
import json
from django.conf import settings
from .hardware_id import get_hardware_id

LICENSE_FILE = os.path.join(settings.BASE_DIR, 'license.key')
SECRET_SALT = "AutoCrosscheck_v1_Secret_Salt_2024" # In production, hide this well!

def generate_license_key(hardware_id):
    """
    Generates a valid license key for a given Hardware ID.
    Format: SIGNATURE|HARDWARE_ID
    """
    raw_string = f"{hardware_id}{SECRET_SALT}"
    signature = hashlib.sha256(raw_string.encode()).hexdigest()
    return f"{signature}|{hardware_id}"

def is_license_valid():
    """
    Checks if the license.key file exists and matches THIS machine.
    """
    if not os.path.exists(LICENSE_FILE):
        return False, "No license file found."

    try:
        with open(LICENSE_FILE, 'r') as f:
            stored_key = f.read().strip()
            
        stored_signature, stored_hwid = stored_key.split('|')
        
        current_hwid = get_hardware_id()
        
        # 1. Check if the key belongs to THIS machine
        if stored_hwid != current_hwid:
            return False, "License is bound to a different machine."
            
        # 2. Verify the signature integrity
        expected_key = generate_license_key(current_hwid)
        if stored_key == expected_key:
            return True, "Valid"
            
        return False, "License file is corrupted."
        
    except Exception:
        return False, "Invalid license format."

def activate_license(input_key):
    """
    Writes the license key to disk if valid.
    In a real app, 'input_key' would come from your online server.
    For this offline version, we simulate the server returning the valid key.
    """
    current_hwid = get_hardware_id()
    expected_key = generate_license_key(current_hwid)
    
    # In a real scenario, the user inputs a code, and we validate it.
    # Here, we assume the input_key passed IS the full signed key string.
    
    if input_key == expected_key:
        with open(LICENSE_FILE, 'w') as f:
            f.write(input_key)
        return True
    return False