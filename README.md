# tap-eventbrite

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap is written in python and works perfectly on Python 3.6. Make sure Python 3.6 is installed in your system.

## Quick Start

1. Install

    It is highly recommended to use a virtualenv to isolate the process without interaction with other python modules.
    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    ```
    ```bash
    > pip install --upgrade singer-python
    > pip install --index-url https://test.pypi.org/simple/ tap-eventbrite
    ```
2. Configure tap-eventbrite
 
    Now, let's create a `tap_config.json` in your working directory, following [tap_config_sample.json](tap_config_sample.json). Tap-Auth0 requires three keys `EVENTBRITE_TOKEN`, `RUN_DAILY` and `ORG_ID`
    
     - `EVENTBRITE_TOKEN` - Your token.
     - `ORG_ID` - Your organization's id
     - `RUN_DAILY` - `True`/`False`. If `True`, the tap just grabs the new data only. If `False`, the tap grabs all the current data in Eventbrite.  

3. Run

  ```bash
› tap-eventbrite -c tap_config.json | target-some-api
```
---

Copyright &copy; 2018 Stitch
