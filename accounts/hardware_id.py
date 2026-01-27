import uuid
import hashlib
import platform
import subprocess

def get_hardware_id():
    """
    Generates a unique, stable Hardware ID based on the machine's specific attributes.
    Returns a hashed string (e.g., 'A1B2-C3D4-E5F6-G7H8').
    """
    system_info = []

    # 1. Platform Node Name (Computer Name)
    system_info.append(platform.node())

    # 2. Machine Architecture
    system_info.append(platform.machine())

    # 3. Processor Details
    system_info.append(platform.processor())

    # 4. MAC Address (Physical Address) - Stable identifier
    mac_num = uuid.getnode()
    mac_address = ':'.join(('%012X' % mac_num)[i:i+2] for i in range(0, 12, 2))
    system_info.append(mac_address)
    
    # 5. OS-Specific Serial Number (More secure)
    try:
        if platform.system() == "Windows":
            cmd = 'wmic csproduct get uuid'
            uuid_serial = subprocess.check_output(cmd).decode().split('\n')[1].strip()
            system_info.append(uuid_serial)
        elif platform.system() == "Darwin": # macOS
            cmd = "ioreg -l | grep IOPlatformSerialNumber"
            # Extract serial from output
            pass 
        elif platform.system() == "Linux":
            # Attempt to read product_uuid
            pass
    except Exception:
        pass # Fallback to standard info if permission denied

    # Combine all collected info into a single string
    raw_string = "|".join(system_info)
    
    # Create a SHA-256 Hash
    hasher = hashlib.sha256(raw_string.encode('utf-8'))
    hash_digest = hasher.hexdigest().upper()
    
    # Format nicely: XXXX-XXXX-XXXX-XXXX
    formatted_id = f"{hash_digest[:4]}-{hash_digest[4:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}"
    
    return formatted_id

if __name__ == "__main__":
    print(f"Machine HWID: {get_hardware_id()}")