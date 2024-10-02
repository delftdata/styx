import setuptools

setuptools.setup(
    name="styx",
    version="0.0.1",
    author="Kyriakos Psarakis",
    packages=setuptools.find_packages(),
    install_requires=[
        # serializers
        'cloudpickle>=3.0.0,<4.0.0',
        'msgspec>=0.18.6,<1.0.0',
        # kafka clients (async, sync)
        'aiokafka>=0.11.0,<1.0',
        'confluent-kafka>=2.5.3,<3.0',
        # async logging
        'aiologger>=0.7.0,<1.0',
        # hashing strings
        'mmh3>=5.0.0,<6.0.0',
    ],
    python_requires='>=3.12',
)
