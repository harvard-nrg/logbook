Building
========
The following documentation will show you how to build Logbook 
into a ``pip`` installable  packages and how to publish that 
package to `PyPI <https://pypi.org>`_.

creating a wheel package
~~~~~~~~~~~~~~~~~~~~~
The ``Makefile`` within the Logbook repository allows you to quickly create 
an installable ``wheel`` package ::

    git clone https://github.com/sbdp/logbook-dptk.git
    cd logbook-dptk
    make dist

You can install this package using ``pip`` for your own testing purposes ::

    pip install dist/logbook_dptk-*-py2.py3-none-any.whl

publishing to pypi.org
~~~~~~~~~~~~~~~~~~~~~~
.. attention::
   You will need to register for a `PyPI account <https://pypi.org/account/register/>`_ 
   to complete this step if you haven't already.

The ``Makefile`` within the Logbook repitory allows you to both create and 
and publish an installable ``wheel`` package to `PyPI <pypi.org>`_ ::

    git clone https://github.com/sbdp/logbook-dptk.git
    cd logbook-dptk 
    make publish

