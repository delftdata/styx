import setuptools

setuptools.setup(
    name="styx",
    version="0.0.1",
    author="Kyriakos Psarakis",
    packages=setuptools.find_packages(),
    install_requires=[
        # serializers
        'cloudpickle>=3.1.1,<4.0.0',
        'msgspec>=0.19.0,<1.0.0',
        # kafka clients (async, sync)
        'aiokafka>=0.12.0,<1.0',
        'confluent-kafka>=2.8.0,<2.8.1',
        # async logging
        'aiologger>=0.7.0,<1.0',
        # hashing strings
        'cityhash>=0.4.8,<1.0.0',
    ],
    python_requires='>=3.13',
)
