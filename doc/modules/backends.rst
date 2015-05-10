********************
bucketcache.backends
********************

.. automodule:: bucketcache.backends
   :members:
   :show-inheritance:

   .. Backend not part of __all__, add it here

   .. autoclass:: bucketcache.backends.Backend
      :show-inheritance:
      :members:

      .. py:attribute:: binary_format

         Return `True` or `False` to indicate if files should be opened in
         binary mode before passing to :py:meth:`from_file` and :py:meth:`dump`.

      .. py:attribute:: default_config

         Associated :py:class:`bucketcache.config.Config` class. Instantiated
         without arguments to create default configuration.

      .. py:attribute:: file_extension

         File extension (without full stop) for files created by the backend.
