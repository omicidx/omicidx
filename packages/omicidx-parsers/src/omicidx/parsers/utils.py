import gzip
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Tuple


def open_file(fname: str, mode: str = 'rt'):
    """open a file, including dealing with gzipped files
 
    Parameters
    ----------
    fname : str
        a filename. If ending in .gz, will use gzip.open(). 
        Otherwise, a regular open() will be used.

    mode : str
        The file opening mode. 
    """
    if(fname.endswith('.gz')):
        f = gzip.open(fname, mode)
    else:
        f = open(fname, mode)
    return(f)


def requests_retry_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: Tuple[int]=(500, 502, 504),
    session: requests.Session = None,
) -> requests.Session:
    """Get a requests session that handles retries

    Parameters
    ==========
    retries: int
        Number of retries allowed
    backoff_factor: float
        Implements geometric backoff 
    status_forcelist: Tuple[int]
        Status codes to retry for
    session: requests.Session 
        Supply a session that will be modified, or 
        a new session will be created.
        
    Returns
    =======
    A new requests.Session with retries enforced.
    ```
t0 = time.time()
try:
    response = requests_retry_session().get(
        'http://httpbin.org/delay/10',
        timeout=5
    )
except Exception as x:
    print('It failed :(', x.__class__.__name__)
else:
    print('It eventually worked', response.status_code)
finally:
    t1 = time.time()
    print('Took', t1 - t0, 'seconds')
    ```
    """

    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session
