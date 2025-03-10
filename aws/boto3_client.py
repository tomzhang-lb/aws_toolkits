# aws boto3 client
import boto3


class Boto3ClientSingleton:
    _instances = {}

    def __new__(cls, service_name, *args, **kwargs):
        if service_name not in cls._instances:
            cls._instances[service_name] = boto3.client(service_name, *args, **kwargs)
        return cls._instances[service_name]