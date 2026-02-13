# aws boto3 client
import boto3


class Boto3ClientSingleton:
    _instances = {}

    session = boto3.Session(profile_name="DataEngineer-859004686855")
    region = session.region_name
    def __new__(cls, service_name, session=session, *args, **kwargs):
        if service_name not in cls._instances:
            cls._instances[service_name] = session.client(service_name, *args, **kwargs)
        return cls._instances[service_name]