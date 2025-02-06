import os
import subprocess

def generate_requirements(base_path):
    # Dossiers spécifiques à lire
    subdirs_to_include = ['airflow/dags', 'api', 'web_app', 'etl', 'tests']
    
    for subdir in subdirs_to_include:
        subdir_path = os.path.join(base_path, subdir)
        if os.path.exists(subdir_path) and os.path.isdir(subdir_path):
            # Vérifie s'il y a des fichiers Python dans le sous-dossier
            for root, dirs, files in os.walk(subdir_path):
                has_python_files = any(file.endswith('.py') for file in files)
                if has_python_files:
                    if subdir == 'airflow/dags':  # Cas particulier pour airflow/dags
                        requirements_file = os.path.join(base_path, 'airflow', 'requirements-airflow.txt')
                        subprocess.run(["pipreqs", root, "--force", "--savepath", requirements_file], check=True)
                        print(f"Fichier généré : {requirements_file}")
                    else:
                        folder_name = os.path.basename(root)
                        requirements_file = os.path.join(root, f"requirements-{folder_name}.txt")
                        subprocess.run(["pipreqs", root, "--force", "--savepath", requirements_file], check=True)
                        print(f"Fichier généré : {requirements_file}")

project_path = os.path.expanduser('~/ProjetPerso/bank-branch-footfall')  # Remplace par ton chemin complet si nécessaire
generate_requirements(project_path)

