import os
from abc import ABCMeta, abstractmethod

import jaydebeapi


class JDBCConnector(metaclass=ABCMeta):
    account_id = os.getenv("ACCOUNT_ID")

    @property
    @abstractmethod
    def role_id(self):
        pass

    @property
    @abstractmethod
    def user(self):
        pass

    @property
    @abstractmethod
    def pwd(self):
        pass

    def connect(self):
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                f"jdbc:ns://{self.account_id}.connect.api.netsuite.com:1708;"
                f"ServerDataSource={self.data_source}.com;"
                "Encrypted=1;"
                f"CustomProperties=(AccountID={self.account_id};RoleID={self.role_id})"
            ),
            {
                "user": self.user,
                "password": self.pwd,
            },
            "NQjc.jar",
        )


class NetSuiteConnector(JDBCConnector):
    data_source = "NetSuite"
    role_id = os.getenv("ROLE_ID")
    user = os.getenv("NS_UID")
    pwd = os.getenv("NS_PWD")


class NetSuite2Connector(JDBCConnector):
    data_source = "NetSuite2"
    role_id = os.getenv("ROLE_ID2")
    user = os.getenv("NS_UID2")
    pwd = os.getenv("NS_PWD2")
