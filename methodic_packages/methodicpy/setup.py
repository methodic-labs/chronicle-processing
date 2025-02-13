from setuptools import setup, find_packages

setup(name='methodicpy',
      version='2.0',
      description='Package for preprocessing Chronicle data via Methodic.',
      author='Kim @ Methodic',
      author_email='kim@getmethodic.com',
      license='GNU GPL v3.0',
      install_requires=[
          'pandas>0.15.0',
          'prefect'
          ],
      packages = find_packages(),
      zip_safe=False)
