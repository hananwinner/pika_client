from setuptools import setup, find_packages

setup(name='pika_client',
      version='0.8',
      description='Pika Client',
      url='https://github.com/BenjiBackslash/pika_client',
      author='Hanan Wiener',
      author_email='hanan888@gmail.com',
      license='MIT',
      packages=find_packages(exclude=['contrib', 'docs', 'test*']),
      install_requires=['pika==0.12.0'],
      zip_safe=False)
