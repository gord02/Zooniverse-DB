"""Example of a simple routine, as it only depends on core analysis functions."""
from zoondb.analysis import seed
from typing import Union


class Simple:
    """Simple Seeder.

    Example
    -------
    >>> from zoondb.routines import simple
    >>> seeds = simple.Simple(flavor="str")
    >>> print(seeds.seedling())
    """

    def __init__(self, flavor: str):
        """Seeder Class.

        Parameters
        ----------
        flavor : str
            Flavor or seed to get,
        """
        self.flavor = flavor

    def seedling(self) -> Union[str, bytes]:
        """Get random seedling.

        Returns
        -------
        str
            Random alpha-numeric seed.
        """
        return seed.get_uuid(flavor=self.flavor)