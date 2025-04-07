Scripts used to purge content from Informatica's Cloud Data Governance Catalog. Since these scripts destroy data in an environment, use at your own risk! 

| Script                          | Description                                                                                                                                                                                   |
|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cdgc_delete_gov_assets.py       | This script purges ALL Governance assets from the catalog. It also can go back X number of days, useful to "roll back" new objects that were created.                                         |
| cdgc_delete_technical_assets.py | This script purges technical assets by running a purge on the Catalog Source Scanner. It can do a specific scanner or all scanners. There is an option to delete the scanner after the purge. |
| cdgc_delete_cdam_assets.py      | This script will purge all CDAM related assets                                                                                                                                                |
| setup.py                        | Various settings used for these type of scripts                                                                                                                                               |
