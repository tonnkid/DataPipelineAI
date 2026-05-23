from setuptools import setup, find_packages

setup(
    name="datapipelineai",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "httpx>=0.27.0",
        "python-dotenv>=1.0.0",
        "pyyaml>=6.0",
        "aiofiles>=23.0",
    ],
    python_requires=">=3.11",
)
