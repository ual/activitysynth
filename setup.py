from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.readlines()
requirements = [item.strip() for item in requirements]

setup(
    name='activitysynth',
    version='0.1.dev0',
    description='Lightweight activity plan generation',
    author='UAL',
    author_email='magardner@berkeley.edu',
    url='https://github.com/ual/activitysynth',
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: BSD License'
    ],
    packages=find_packages(),
    install_requires=requirements,
    dependency_links=[
        'git+https://github.com/udst/choicemodels/archive/master.zip#egg=choicemodels',
        'git+https://github.com/udst/urbansim_templates/archive/master.zip#egg=urbansim_templates']
)