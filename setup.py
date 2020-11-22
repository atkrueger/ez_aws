from setuptools import setup

setup(
    name='ez_aws',
    version='0.1',
    description='easy access to AWS resources',
    url='https://github.com/atkrueger/ez_aws',
    author='atk',
    author_email='atkrueger@gmail.com',
    license='MIT',
    packages=['ez_aws'],
    install_requires=['boto3', 'smart_open', 'requests']
)