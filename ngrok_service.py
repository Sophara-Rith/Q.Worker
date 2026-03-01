import subprocess
import time
import sys

def start_app():
    print("🚀 Starting AutoCrosscheck Local Server...")
    # Start Django on port 78512
    django_proc = subprocess.Popen([sys.executable, "manage.py", "runserver", "0.0.0.0:3030"])
    
    print("🌐 Starting ngrok Tunnel...")
    try:
        # Assumes ngrok is installed in your PATH
        # 'http 3030' creates the tunnel
        ngrok_proc = subprocess.Popen(["ngrok", "http", "3030"])
        
        print("\n✅ System Running!")
        print("1. Check the ngrok terminal window for your public URL (e.g., https://xyz.ngrok-free.app)")
        print("2. Send that URL to your client.")
        print("3. Keep this window open during testing.\n")
        
        django_proc.wait()
    except KeyboardInterrupt:
        django_proc.terminate()
        print("\n🛑 Shutting down...")

if __name__ == "__main__":
    start_app()