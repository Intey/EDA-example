from setuptools import setup  # type: ignore

setup(
    name="ebus_adapter",
    version="0.1",
    url="https://github.com/intey/EDA-example",
    license="MIT",
    author="Intey",
    author_email="ziexe0@gmail.com",
    description="Common bus adapter",
    packages=["ebus",],
    long_description=open("README.md").read(),
    zip_safe=False,
)
