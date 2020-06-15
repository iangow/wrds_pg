Step by Step Instructions to Use wrds2pg in Windows 10
=========

This manual includes step by step instructions on how to set up a PostgreSQL database on your computer and feed it with SAS datasets, either automatically pulled from [WRDS](https://wrds-web.wharton.upenn.edu/wrds/) or generated locally.

## Step 1. Install softwares

-	Download and install PostgreSQL from [here](https://www.postgresql.org/download/windows/). Write down your PostgreSQL database and user name, which we will need later.
-	Download and install Anaconda from [here](https://www.anaconda.com/products/individual);
-	Type in “anaconda” in the search bar located at the left bottom corner; some apps will pop up, click on the right arrow on anaconda prompt (anaconda3), anaconda powershell prompt (anaconda3), spyder (anaconda3), and jupyter Notebook (anaconda3), and click “pin to taskbar”. We will need them later;
-	Click the anaconda prompt (anaconda3) on the taskbar, in the prompt window, type the following and press enter:  `conda update anaconda`
-	Stay in the anaconda prompt window, type the following and press enter: `conda install spyder=4.1.3`
-	In the anaconda prompt (anaconda3), type the following and press enter to verify that Pip has been installed correctly (pip version info will show): `pip -V`
-	In the anaconda prompt (anaconda3), type the following and press enter to verify that Python3 has been installed correctly (python3 version info will show): `python`
-	Click the anaconda prompt (anaconda3) on the taskbar, in the prompt window, type the following and press enter to install wrds2pg:  `pip install wrds2pg`

## Step 2. Set up environment variables in Windows 10

-	Type in “Control Panel” in the search bar located at the left bottom corner, and press enter;
-	In the prompt “Control Panel” window, click “System and Security”, then click “system”, then click Advanced system settings;
-	In the prompt “System Properties-Advanced” tab, click “Environment Variables”;
-	In the prompt “Environment Variables” window, under “System variables”, click “New”;
-	In the prompt “New System Variable” window, type in a pair of variable name and variable value, and click “OK” one by one. Then click “OK” again to save changes. For example, to use the python wrds2pg package, we type in the following pairs one by one (4 times):
|     variable name    |     variable value                                                                                                                     |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------|
|     PGDATABASE       |     The name of the PostgreSQL database you use                                                                                        |
|     PGUSER           |     Your username on the PostgreSQL database                                                                                           |
|     PGHOST           |     Where the PostgreSQL database is to be found (this will be localhost if it’s on the same machine as you're running the code on)    |
|     WRDS_ID          |     Your WRDS ID                                                                                                                       |
|     PGPASSWORD       |     Your password for the PostgreSQL database                                                                                          |

## Step 3. Set up a public key and copy to the WRDS server

- Click anaconda powershell prompt (anaconda3) on the taskbar, type the following and press enter: `ssh-keygen -t rsa`    Accept defaults (i.e., just press enter) at prompts, a pair of key files with no passphrase will be generated. It needs to be set up this way so that the key files can be used without being "unlocked" with a password, and the scripts can run without user intervention.
- Stay in the anaconda powershell prompt (anaconda3) window, type the following and press enter to log into your WRDS account via SSH: `ssh YourWrdsId@wrds-cloud.wharton.upenn.edu`  Type your password at prompt. Then use `mkdir ~/.ssh` to create a directory .ssh in your WRDS home directory. (Skip this step if you already have .ssh directory set up)
- Stay in the anaconda powershell prompt (anaconda3) window, type the following and press enter to append your public key to your account at the WRDS server: `cat ~/.ssh/id_rsa.pub | ssh iangow@wrds-cloud.wharton.upenn.edu "cat >> ~/.ssh/authorized_keys"` Type your password at prompt if needed.
- For final check, click anaconda powershell prompt (anaconda3) on the taskbar, use `cat ~/.ssh/id_rsa.pub` to reveal the public key on your computer, and `ssh YourWrdsId @wrds-cloud.wharton.upenn.edu cat ~/.ssh/authorized_keys` to reveal all authorized keys on WRDS server, the public key on your computer should have been appended to the authorized_keys on the WRDS server.

## Step 4. Test wrds2pg

- Click either spyder (anaconda3) or jupyter Notebook (anaconda3) on the taskbar, then run the following Python code as examples:

```py
from wrds2pg import wrds_update

# 1. Download crsp.mcti from wrds and upload to pg as crps.mcti
# Simplest version
wrds_update(table_name="mcti", schema="public")

# Tailored arguments 
wrds_update(table_name="mcti", schema="public", 
	fix_missing=True, fix_cr=True, drop="b30ret b30ind", obs=10, 
	rename="caldt=calendar_date", force=True)

# 2. Upload test.sas7dbat to pg as crsp.mcti
wrds_update( table_name="chars", schema="public", fpath="YOURFILEPATH")
```

