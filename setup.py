from setuptools import setup, find_packages

setup(
    name="marketing_science",
    version="0.1",
    packages=find_packages(where="src"),  # Include all packages under src/
    package_dir={"": "src"},  # Tells setuptools packages are under src
    install_requires=[
        "pandas",
        "pymongo",
        # Add other dependencies here
    ],
    entry_points={
        "console_scripts": [
            "your_project=src.main:main",  # Main script entry point
        ],
    },
    description="Marketing Science toolkit",
    author="Izzaz Iskandar",
    author_email="izzaz76230@gmail.com",
)
