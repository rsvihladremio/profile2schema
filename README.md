# profile2schema

 CLI that supports reading a dremio profile.json (text or zip file containing the profile) and either outputs arrow schema represented as JSON to standard out or to a directory.

## How to use

### output to standard out

    profile2schema  ~/Downloads/my-profile.zip    

### output to folder

    profile2schema  ~/Downloads/my-profile.zip -o ~/Downloads/my-schema

## LICENSE
 
Apache License Version 2.0

