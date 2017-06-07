"""
A Spawner for JupyterHub that runs each user's server in a separate docker container
"""

import string
from textwrap import dedent
from concurrent.futures import ThreadPoolExecutor
from pprint import pformat

import docker
from docker.errors import APIError
from docker.utils import kwargs_from_env
from tornado import gen

from escapism import escape
from jupyterhub.spawner import Spawner
from traitlets import (
    Dict,
    Unicode,
    Bool,
    Int,
    Any,
    default,
    observe,
)

from .volumenamingstrategy import default_format_volume_name

class UnicodeOrFalse(Unicode):
    info_text = 'a unicode string or False'
    def validate(self, obj, value):
        if value is False:
            return value
        return super(UnicodeOrFalse, self).validate(obj, value)

class DockerSpawner(Spawner):

    _executor = None
    @property
    def executor(self):
        """single global executor"""
        cls = self.__class__
        if cls._executor is None:
            cls._executor = ThreadPoolExecutor(1)
        return cls._executor

    _client = None
    @property
    def client(self):
        """single global client instance"""
        cls = self.__class__
        if cls._client is None:
            kwargs = {}
            if self.tls_config:
                kwargs['tls'] = docker.tls.TLSConfig(**self.tls_config)
            kwargs.update(kwargs_from_env())
            client = docker.APIClient(version='auto', **kwargs)
            cls._client = client
        return cls._client

    service_id = Unicode()
    container_port = Int(8888, min=1, max=65535, config=True)
    container_image = Unicode("jupyterhub/singleuser", config=True)
    container_prefix = Unicode(
        "jupyter",
        config=True,
        help=dedent(
            """
            Prefix for container names. The full container name for a particular
            user will be <prefix>-<username>.
            """
        )
    )

    volumes = Dict(
        config=True,
        help=dedent(
            """
            Map from host file/directory to container (guest) file/directory
            mount point and (optionally) a mode. When specifying the
            guest mount point (bind) for the volume, you may use a
            dict or str. If a str, then the volume will default to a
            read-write (mode="rw"). With a dict, the bind is
            identified by "bind" and the "mode" may be one of "rw"
            (default), "ro" (read-only), "z" (public/shared SELinux
            volume label), and "Z" (private/unshared SELinux volume
            label).

            If format_volume_name is not set,
            default_format_volume_name is used for naming volumes.
            In this case, if you use {username} in either the host or guest
            file/directory path, it will be replaced with the current
            user's name.
            """
        )
    )

    read_only_volumes = Dict(
        config=True,
        help=dedent(
            """
            Map from host file/directory to container file/directory.
            Volumes specified here will be read-only in the container.

            If format_volume_name is not set,
            default_format_volume_name is used for naming volumes.
            In this case, if you use {username} in either the host or guest
            file/directory path, it will be replaced with the current
            user's name.
            """
        )
    )

    format_volume_name = Any(
        help="""Any callable that accepts a string template and a DockerSpawner instance as parameters in that order and returns a string.

        Reusable implementations should go in dockerspawner.VolumeNamingStrategy, tests should go in ...
        """
    ).tag(config=True)

    def default_format_volume_name(template, spawner):
        return template.format(username=spawner.user.name)

    @default('format_volume_name')
    def _get_default_format_volume_name(self):
        return default_format_volume_name

    use_docker_client_env = Bool(True, config=True,
        help="DEPRECATED. Docker env variables are always used if present.")
    @observe('use_docker_client_env')
    def _client_env_changed(self):
        self.log.warning("DockerSpawner.use_docker_client_env is deprecated and ignored."
        "  Docker environment variables are always used if defined.")
    tls_config = Dict(config=True,
        help="""Arguments to pass to docker TLS configuration.

        See docker.client.TLSConfig constructor for options.
        """
    )
    tls = tls_verify = tls_ca = tls_cert = \
    tls_key = tls_assert_hostname = Any(config=True,
        help="""DEPRECATED. Use DockerSpawner.tls_config dict to set any TLS options."""
    )
    @observe('tls', 'tls_verify', 'tls_ca', 'tls_cert', 'tls_key', 'tls_assert_hostname')
    def _tls_changed(self, change):
        self.log.warning("%s config ignored, use %s.tls_config dict to set full TLS configuration.",
            change.name, self.__class__.__name__,
        )

    extra_create_kwargs = Dict(config=True, help="Additional args to pass for service create")
    extra_host_config = Dict(config=True, help="Additional args to create_host_config for container create")

    _container_safe_chars = set(string.ascii_letters + string.digits + '-')
    _container_escape_char = '_'

    hub_ip_connect = Unicode(
        config=True,
        help=dedent(
            """
            If set, DockerSpawner will configure the containers to use
            the specified IP to connect the hub api.  This is useful
            when the hub_api is bound to listen on all ports or is
            running inside of a container.
            """
        )
    )

    links = Dict(
        config=True,
        help=dedent(
            """
            Specify docker link mapping to add to the container, e.g.

                links = {'jupyterhub': 'jupyterhub'}

            If the Hub is running in a Docker container,
            this can simplify routing because all traffic will be using docker hostnames.
            """
        )
    )

    @property
    def tls_client(self):
        """A tuple consisting of the TLS client certificate and key if they
        have been provided, otherwise None.

        """
        if self.tls_cert and self.tls_key:
            return (self.tls_cert, self.tls_key)
        return None

    @property
    def volume_mount_points(self):
        """
        Volumes are declared in docker-py in two stages.  First, you declare
        all the locations where you're going to mount volumes when you call
        create_container.
        Returns a sorted list of all the values in self.volumes or
        self.read_only_volumes.
        """
        return sorted([value['bind'] for value in self.volume_binds.values()])

    @property
    def volume_binds(self):
        """
        The second half of declaring a volume with docker-py happens when you
        actually call start().  The required format is a dict of dicts that
        looks like:

        {
            host_location: {'bind': container_location, 'mode': 'rw'}
        }
        mode may be 'ro', 'rw', 'z', or 'Z'.

        """
        binds = self._volumes_to_binds(self.volumes, {})
        return self._volumes_to_binds(self.read_only_volumes, binds, mode='ro')

    _escaped_name = None
    @property
    def escaped_name(self):
        if self._escaped_name is None:
            self._escaped_name = escape(self.user.name,
                safe=self._container_safe_chars,
                escape_char=self._container_escape_char,
            )
        return self._escaped_name

    @property
    def service_name(self):
        return "{}-{}".format(self.container_prefix, self.escaped_name)

    def load_state(self, state):
        super(DockerSpawner, self).load_state(state)
        self.service_id = state.get('service_id', '')

    def get_state(self):
        state = super(DockerSpawner, self).get_state()
        if self.service_id:
            state['service_id'] = self.service_id
        return state

    def _public_hub_api_url(self):
        proto, path = self.hub.api_url.split('://', 1)
        ip, rest = path.split(':', 1)
        return '{proto}://{ip}:{rest}'.format(
            proto = proto,
            ip = self.hub_ip_connect,
            rest = rest
        )

    def _env_keep_default(self):
        """Don't inherit any env from the parent process"""
        return []

    def get_env(self):
        env = super(DockerSpawner, self).get_env()
        env.update(dict(
            JPY_USER=self.user.name,
            JPY_COOKIE_NAME=self.user.server.cookie_name,
            JPY_BASE_URL=self.user.server.base_url,
            JPY_HUB_PREFIX=self.hub.server.base_url
        ))

        if self.notebook_dir:
            env['NOTEBOOK_DIR'] = self.notebook_dir

        if self.hub_ip_connect:
           hub_api_url = self._public_hub_api_url()
        else:
           hub_api_url = self.hub.api_url
        env['JPY_HUB_API_URL'] = hub_api_url

        return env

    def _docker(self, method, *args, **kwargs):
        """wrapper for calling docker methods

        to be passed to ThreadPoolExecutor
        """
        m = getattr(self.client, method)
        return m(*args, **kwargs)

    def docker(self, method, *args, **kwargs):
        """Call a docker method in a background thread

        returns a Future
        """
        return self.executor.submit(self._docker, method, *args, **kwargs)

    @gen.coroutine
    def poll(self):
        """Check for my id in `docker ps`"""
        service = yield self.get_service()
        if not service:
            self.log.warn("service not found")
            return ""
        return None

    @gen.coroutine
    def get_service(self):
        self.log.debug("Getting service '%s'", self.service_name)
        try:
            service = yield self.docker(
                'inspect_service', self.service_name
            )
            self.service_id = service['ID']
        except APIError as e:
            if e.response.status_code == 404:
                self.log.info("Service '%s' is gone", self.service_name)
                service = None
                # my service is gone, forget my id
                self.service_id = ''
            elif e.response.status_code == 500:
                self.log.info("Service '%s' is on unhealthy node", self.service_name)
                service = None
                # my service is unhealthy, forget my id
                self.service_id = ''
            else:
                raise
        return service

    @gen.coroutine
    def start(self, image=None, extra_create_kwargs=None,
        extra_start_kwargs=None, extra_host_config=None):
        """Start the single-user server in a docker container. You can override
        the default parameters passed to `create_container` through the
        `extra_create_kwargs` dictionary and passed to `start` through the
        `extra_start_kwargs` dictionary.  You can also override the
        'host_config' parameter passed to `create_container` through the
        `extra_host_config` dictionary.

        Per-instance `extra_create_kwargs`, `extra_start_kwargs`, and
        `extra_host_config` take precedence over their global counterparts.

        """
        service = yield self.get_service()
        if service is None:
            image = image or self.container_image

            # build the dictionary of keyword arguments for create_service
            mounts = [{'Source': host_dir,
                       'Target': container['bind'],
                       'Type': 'bind',
                       'ReadOnly': container['ro']} \
                       for host_dir, container in self.volume_binds.items()]
            containerspec_kwargs = dict(
                Image=image,
                Env=['{}={}'.format(k, v) for k, v in self.get_env().items()],
                Mounts=mounts
            )
            containerspec_kwargs.update(self.extra_create_kwargs)
            if extra_create_kwargs:
                containerspec_kwargs.update(extra_create_kwargs)
            if image.startswith('jupyter/') and 'Command' not in create_kwargs:
                # jupyter/docker-stacks launch with /usr/local/bin/start-singleuser.sh
                # use this as default if any jupyter/ image is being used.
                containerspec_kwargs['Command'] = '/usr/local/bin/start-singleuser.sh'

            # TODO: Import volume_binds and links to args for create_service
            self.log.info('volume_binds={}, links={}'.format(self.volume_binds,
                                                             self.links))
            endpointspec = {'Ports': [{'TargetPort': self.container_port}]}

            resources = {}
            if hasattr(self, 'mem_limit') and self.mem_limit is not None:
                resources['Limits'] = {'MemoryBytes': self.mem_limit}

            endpointspec.update(self.extra_host_config)

            if extra_host_config:
                endpointspec.update(extra_host_config)

            create_kwargs = dict(
                name=self.service_name,
                task_template={'ContainerSpec': containerspec_kwargs,
                               'Resources': resources},
                endpoint_spec=endpointspec
            )

            self.log.info("Starting host with config: %s", repr(create_kwargs))

            # create the service
            resp = yield self.docker('create_service', **create_kwargs)
            self.service_id = resp['ID']
            self.log.info(
                "Created service '%s' (id: %s) from image %s",
                self.service_name, self.service_id[:7], image)

        else:
            self.log.info(
                "Found existing service '%s' (id: %s)",
                self.service_name, self.service_id[:7])
            # Handle re-using API token.
            # Get the API token from the environment variables
            # of the running service:
            for line in service['Spec']['TaskTemplate']['ContainerSpec']['Env']:
                if line.startswith('JPY_API_TOKEN='):
                    self.api_token = line.split('=', 1)[1]
                    break

        ip, port = yield self.get_ip_and_port()
        # store on user for pre-jupyterhub-0.7:
        self.user.server.ip = ip
        self.user.server.port = port
        # jupyterhub 0.7 prefers returning ip, port:
        return (ip, port)

    @gen.coroutine
    def get_ip_and_port(self):
        """Queries Docker daemon for container's IP and port.
        """
        retries = 3
        while retries > 0:
            resp = yield self.docker('inspect_service', self.service_id)
            endpoint = resp['Endpoint']
            if 'Ports' in endpoint:
                break
            self.log.info('The published ports are not assigned... Waiting 3 secs')
            yield gen.sleep(3)
            retries -= 1
        self.log.info('Endpoint: Ports=%s', endpoint['Ports'])
        # JupyterHub connects to the container via routing mesh
        ip = '127.0.0.1'
        port = endpoint['Ports'][0]['PublishedPort']
        return ip, port

    @gen.coroutine
    def stop(self, now=False):
        """Remove the service

        Consider using pause/unpause when docker-py adds support
        """
        self.log.info(
            "Removing service %s (id: %s)",
            self.service_name, self.service_id[:7])
        yield self.docker('remove_service', self.service_id)
        self.will_resume = False

        self.clear_state()

    def _volumes_to_binds(self, volumes, binds, mode='rw'):
        """Extract the volume mount points from volumes property.

        Returns a dict of dict entries of the form::

            {'/host/dir': {'bind': '/guest/dir': 'mode': 'rw'}}
        """
        def _fmt(v):
            return self.format_volume_name(v, self)

        for k, v in volumes.items():
            m = mode
            if isinstance(v, dict):
                if 'mode' in v:
                    m = v['mode']
                v = v['bind']
            binds[_fmt(k)] = {'bind': _fmt(v), 'mode': m}
        return binds
