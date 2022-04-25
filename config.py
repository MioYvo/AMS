from dynaconf import Dynaconf

settings = Dynaconf(
    environments=True,
    envvar_prefix="AMS",    # AMS means AssetManagementService
    settings_files=['settings.toml'],
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load these files in the order.
