import setuptools

setuptools.setup(
    name="styx",
    version="0.0.1",
    author="Kyriakos Psarakis",
    packages=setuptools.find_packages(),
    install_requires=[
        # serializers
        'cloudpickle>=2.1.0,<4.0.0',
        'msgspec>=0.18.5,<1.0.0',
        # kafka clients (async, sync)
        'aiokafka>=0.10.0,<1.0',
        'confluent-kafka>=2.3.0,<3.0',
        # async logging
        'aiologger>=0.7.0,<1.0',
        # hashing strings
        'mmh3>=4.1.0,<5.0.0',
    ],
    python_requires='>=3.11',
)
