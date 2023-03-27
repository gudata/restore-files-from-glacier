# Extracting multiple files from Glacier

When you have millions or thousands of files, extracting them from glacier is not trivial task

Here is what I use as reliable process.

Steps:

- generate list of the files from glacier
- request the export from aws api
- copy them locally or on another s3 location


All the commands needs to be run in a poety shell or with poetry run app.py

## Generating a list of files

Just use the list.py

    ./list.py somebucket.name.com --prefix="optional/prefix" --storage_class=GLACIER > glacier-keys

a [sample file](./keys) will generated

## Request the export

The export is done in parallel.

    cat glacier-keys | ./parallel.py somebucket.name.com --tier=Bulk

If you use --tier=fastest then we will query for the storage class, and if it is GLACIER - standard tier will be used.
If it is DEEP_ARCHIVE a bulk tier will be used.


## Finally store them.

The best way is to use the recursive commands from the aws-cli "sync " and "cp --recursive" but you have to also use
 --force-glacier-transfer flag.

This will ignore the check for the storage class and the copy/sync will proceed.

example will be

    aws s3 sync --force-glacier-transfer s3://somebucket.name.com export/


## What to watch

While you are "exporting", more files will be transitioned to glacier
