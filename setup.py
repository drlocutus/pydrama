#!/local/python/bin/python2

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

ext_modules = [
    Extension("drama.__drama__", ["src/drama.pyx"],
        depends=['setup.py', 'src/drama.pxd', 'src/ditsaltin.h'],
        include_dirs=['./',
            '/jac_sw/drama/CurrentRelease/include',
            '/jac_sw/drama/CurrentRelease/include/os/Linux-x86_64',
            '/jac_sw/itsroot/install/common/include'],
        library_dirs=['./',
            '/jac_sw/drama/CurrentRelease/lib/Linux-x86_64',
            '/jac_sw/itsroot/install/common/lib/Linux-x86_64',
            '/jac_sw/epics/CurrentRelease/lib/Linux-x86_64'],
        libraries=['jit', 'expat', 'tide', 'ca', 'Com', 'git',
                   'dul', 'dits', 'imp', 'sds', 'ers', 'mess', 'm'],
        define_macros=[("unix",None),("DPOSIX_1",None),
                       ("_GNU_SOURCE",None),("UNIX",None)]
        )]

setup(
  name = 'DRAMA for Python',
  author = 'Ryan Berthold',
  cmdclass = {'build_ext': build_ext},
  packages = ['drama'],
  ext_modules = ext_modules
)


### SAMPLE SETUP COMMANDS ###
#
# build without installing:
# ./setup.py build_ext --inplace
#
# install:
# ./setup.py install
#

