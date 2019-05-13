# Metabase Teradata Driver (Community-Supported)

The Metabase Teradata driver allows Metabase v0.32.7 or above to
connect to [Teradata](https://www.teradata.com/) databases.
Instructions for installing it can be found below.

This driver is community-supported and is not considered part of the
core Metabase project. If you would like to open a GitHub issue to
report a bug or request new features, or would like to open a pull
requests against it, please do so in this repository, and not in the
core Metabase GitHub repository.

## Obtaining the Teradata Driver

### Where to find it

[Click here](https://github.com/swisscom-bigdata/metabase-teradata-driver/releases/latest) to view the latest release of the Metabase Teradata driver; click the link to download `teradata.metabase-driver.jar`.

You can find past releases of the Teradata driver [here](https://github.com/swisscom-bigdata/metabase-teradata-driver/releases).


### How to Install it

Metabase will automatically make the Teradata driver available if it finds the driver and the proprietary jdbc JARs in the Metabase plugins directory when it starts up.
All you need to do is create the directory `plugins` (if it's not already there), move the JAR you just downloaded into it, and restart Metabase.

By default, the plugins directory is called `plugins`, and lives in the same directory as the Metabase JAR.

For example, if you're running Metabase from a directory called `/app/`, you should move the Teradata driver and the proprietary jdbc JARs to `/app/plugins/`:

```bash
# example directory structure for running Metabase with Teradata support
/app/metabase.jar
/app/plugins/teradata.metabase-driver.jar
/app/plugins/tdgssconfig.jar
/app/plugins/terajdbc4.jar
```

If you're running Metabase from the Mac App, the plugins directory defaults to `~/Library/Application Support/Metabase/Plugins/`:

```bash
# example directory structure for running Metabase Mac App with Teradata support
/Users/you/Library/Application Support/Metabase/Plugins/teradata.metabase-driver.jar
/Users/you/Library/Application Support/Metabase/Plugins/tdgssconfig.jar
/Users/you/Library/Application Support/Metabase/Plugins/terajdbc4.jar
```

If you are running the Docker image or you want to use another directory for plugins, you should specify a custom plugins directory by setting the environment variable `MB_PLUGINS_DIR`.


## Building the Teradata Driver Yourself

### Prereqs: Install Metabase locally, compiled for building drivers

```bash
cd /path/to/metabase/source
lein install-for-building-drivers
```

### Build it

```bash
cd /path/to/teradata-driver
lein clean
DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar
```

This will build a file called `target/uberjar/teradata.metabase-driver.jar`; copy this to your Metabase `./plugins` directory.