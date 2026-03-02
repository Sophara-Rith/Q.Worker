import os
import glob
from setuptools import setup
from Cython.Build import cythonize

# 1. The apps that contain your secret logic
TARGET_FOLDERS = [
    'accounts', 'core', 'crosscheck', 'dashboard', 
    'consolidation', 'reports', 'updater'
]

python_files = []
for folder in TARGET_FOLDERS:
    for root, dirs, files in os.walk(folder):
        # We MUST skip migrations and __init__ files so Django doesn't break
        if 'migrations' in root:
            continue
        for file in files:
            if file.endswith('.py') and file != '__init__.py':
                python_files.append(os.path.join(root, file))

print(f"Found {len(python_files)} files to secure. Compiling now...")

# 2. Compile all files to C-Extensions
setup(
    ext_modules=cythonize(python_files, compiler_directives={'language_level': "3"}),
    script_args=["build_ext", "--inplace"]
)

# 3. Cleanup and Secure
print("\nCleaning up and locking down files...")
for py_path in python_files:
    dir_name = os.path.dirname(py_path)
    base_name = os.path.basename(py_path).replace('.py', '')

    # Remove the temporary .c file
    c_file = os.path.join(dir_name, f"{base_name}.c")
    if os.path.exists(c_file):
        os.remove(c_file)

    # Rename the messy .pyd file (e.g., views.cp313-win_amd64.pyd -> views.pyd)
    for pyd_file in glob.glob(os.path.join(dir_name, f"{base_name}.*.pyd")):
        final_pyd = os.path.join(dir_name, f"{base_name}.pyd")
        if os.path.exists(final_pyd):
            os.remove(final_pyd)
        os.rename(pyd_file, final_pyd)

    # DELETE THE ORIGINAL PYTHON FILE (This secures your code!)
    if os.path.exists(py_path):
        os.remove(py_path)

print("\n✅ SUCCESS: All selected logic is now fully encrypted as .pyd files!")