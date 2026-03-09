import os
import sys
import subprocess
import time
import webbrowser
import threading

def open_browser():
    # Wait 3 seconds for the Django server to start, then open the browser
    time.sleep(3)
    webbrowser.open("http://127.0.0.1:49854/dashboard/")

def print_splash():
    # Clear the screen and set colors (Cyan text on Black background)
    os.system('cls' if os.name == 'nt' else 'clear')
    os.system('title AuditCore PRO - Server Running & color 0B')
    os.system('chcp 65001 >nul') # Ensure ASCII art renders perfectly
    
    banner = """
     █████╗ ██╗   ██╗██████╗ ██╗████████╗ ██████╗ ██████╗ ██████╗ ███████╗
    ██╔══██╗██║   ██║██╔══██╗██║╚══██╔══╝██╔════╝██╔═══██╗██╔══██╗██╔════╝
    ███████║██║   ██║██║  ██║██║   ██║   ██║     ██║   ██║██████╔╝█████╗  
    ██╔══██║██║   ██║██║  ██║██║   ██║   ██║     ██║   ██║██╔══██╗██╔══╝  
    ██║  ██║╚██████╔╝██████╔╝██║   ██║   ╚██████╗╚██████╔╝██║  ██║███████╗
    ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚═╝   ╚═╝    ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚══════╝
                                                                      
                         ██████╗ ██████╗  ██████╗ 
                         ██╔══██╗██╔══██╗██╔═══██╗
                         ██████╔╝████████╔╝██║   ██║
                         ██╔═══╝ ██╔══██╗██║   ██║
                         ██║     ██║  ██║╚██████╔╝
                         ╚═╝     ╚═╝  ╚═╝ ╚═════╝ 
    """
    print(banner)
    print("=========================================================================")
    print("   AuditCore PRO Engine is currently running...")
    print("   Your web browser will open automatically.")
    print("")
    print("   [!] TO STOP THE SERVER: Simply close this window (Click the X)")
    print("=========================================================================\n")

def main():
    # Get the actual installation folder
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
    venv_dir = os.path.join(base_dir, "venv")
    
    # Paths to the virtual environment executables
    python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
    pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
    manage_py = os.path.join(base_dir, "manage.py")

    # =================================================================
    # 1. FIRST TIME SETUP (Builds the environment if it doesn't exist)
    # =================================================================
    if not os.path.exists(python_exe):
        os.system('title AuditCore PRO - Initial Setup & color 0B & chcp 65001 >nul')
        print("=========================================================================")
        print("                   AuditCore PRO - System Installation                   ")
        print("=========================================================================")
        print("\n[SETUP] First-time launch detected. Building local environment...")
        subprocess.run(["python", "-m", "venv", "venv"], cwd=base_dir)
        
        print("\n[SETUP] Installing required system libraries...")
        subprocess.run([pip_exe, "install", "-r", "requirements.txt"], cwd=base_dir)
        
        print("\n[SETUP] Environment configured successfully! Starting System...\n")
        time.sleep(2)

    # =================================================================
    # 2. SHOW SPLASH SCREEN & INSTRUCTIONS
    # =================================================================
    print_splash()

    # =================================================================
    # 3. START WEB BROWSER
    # =================================================================
    threading.Thread(target=open_browser, daemon=True).start()

    # =================================================================
    # 4. RUN DJANGO IN THE CURRENT WINDOW
    # =================================================================
    # Because we removed CREATE_NO_WINDOW, the server locks into this current
    # visible window. When the user closes the window, the server dies with it!
    try:
        subprocess.run(
            [python_exe, manage_py, "runserver", "49854", "--noreload"], 
            cwd=base_dir,
            stdout=subprocess.DEVNULL,   # Mutes the standard Django startup text & HTTP logs
            stderr=subprocess.DEVNULL    # Mutes the red "WARNING: Development server" text
        )
    except KeyboardInterrupt:
        # Catches Ctrl+C if they try to shut it down via keyboard
        print("\nShutting down AuditCore PRO...")
        sys.exit(0)

if __name__ == "__main__":
    main()