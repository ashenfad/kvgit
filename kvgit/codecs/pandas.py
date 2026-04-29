"""Pandas codec — currently a thin alias for the numpy codec.

When pandas pickles a ``DataFrame`` or ``Series``, its
``BlockManager.__reduce__`` exposes the per-block ndarrays as Python
objects. Pickle visits those ndarrays before reducing them, which is
exactly when our ``persistent_id`` hook fires — the numpy codec then
catches the block buffers and chunks them.

Net effect: ``compose(NumpyCodec(), ...)`` already deduplicates the
underlying buffers of DataFrames and Series with no extra code. We
re-export under a pandas-flavored name for clarity in user code.

Caveats (no-op cases that fall back to opaque pickle, no chunking):

* Extension dtypes whose pickle path doesn't expose an ndarray:
  ``ArrowDtype``, ``CategoricalDtype.codes`` is ndarray-backed
  (so it chunks), but ``categories`` may not always; ``MaskedArray``
  surfaces ``.data`` and ``.mask`` ndarrays cleanly.
* DataFrames with non-default attrs / index types may grow the
  pickle size slightly because the non-block parts still pickle in
  full. The block buffers — usually 99% of the bytes — still chunk.
"""

from __future__ import annotations

from .numpy import NumpyCodec as PandasCodec

__all__ = ["PandasCodec"]
