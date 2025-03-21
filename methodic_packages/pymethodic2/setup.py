from setuptools import setup, find_packages

setup(name='pymethodic2',
      version='1.0',
      description='Package for preprocessing Chronicle data via Methodic.',
      author='Kim @ Methodic',
      author_email='kim@getmethodic.com',
      license='GNU GPL v3.0',
      install_requires=[
          'pandas>0.15.0',
          ],
      packages = find_packages(),
      zip_safe=False)
