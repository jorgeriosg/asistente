#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mods import config as env
from mods import datadog as dg
from traceback import format_exc as errorTrace

import pymysql
import warnings


class SqlConnection(object):
    """
    Sql connection module, made for disabling auto commit function
    in order to make a lot of querys with the posibility to roll back if needed.

    It will create a connection at the instance creation, so remember to close
    the connection when you are done using the class instance.
    """
    mysql_connection = {}

    def __init__(self):
        self.mysql_connection = pymysql.connect(
            host=env.MYSQL_HOST,
            user=env.MYSQL_USER,
            password=env.MYSQL_PASS,
            db=env.MYSQL_DB,
            charset='utf8',
            use_unicode=True,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False
        )

    def commit_changes(self) -> None:
        """
        Only commit changes to the database
        """
        self.mysql_connection.commit()

    def roll_back_changes(self) -> None:
        """
        Only roll back changes from the connection
        """
        self.mysql_connection.rollback()

    def close_connection(self) -> None:
        """
        Only closes the connection to the database
        """
        self.mysql_connection.close()

    def commit_and_close(self) -> None:
        """
        Commit changes and close the database connection
        """
        self.mysql_connection.commit()
        self.mysql_connection.close()

    def roll_back_and_close(self) -> None:
        """
        Roll back changes and close the database connection
        """
        self.mysql_connection.rollback()
        self.mysql_connection.close()

    def find(self, query: str, params: dict = None) -> list:
        """
        Function that will find one or many records in the database returning a list.

        Args:

            query (str): The query to execute.
            params (dict, optional): The query params. Defaults to None.

        Returns:

            list: List of data returned, if an error is raised, an empty list will be returned
        """
        try:
            with self.mysql_connection.cursor() as cursor:
                cursor.execute(query, params)
                data = cursor.fetchall()
                return list() if data == tuple() else data
        except Exception as err:
            desc = "Unexpected error at SqlConnection class find: " + str(err)
            output_object = {
                "Message": desc,
                "Query": query,
                "Traceback": errorTrace()
            }
            dg.log_datadog(output_object)
            return list()

    def alter(self, query: str, params: dict = None, returnId: bool = False, sendDatadogQuery: bool = True) -> bool:
        """
        Function that will INSERT, UPDATE, or DELETE statements in the database with params if needed
        It handles WARNINGS with 'Duplicated entry' as successful execution.

        Args:

            query (str): The query to execute.
            params (dict, optional): The query params. Defaults to None.
            returnId (bool, optional): If the lastRowId is needed. Defaults to False.
            sendDatadogQuery (bool, optional): If the query it's too long set it to False. Defaults to True.

        Returns:

            bool: Returns the success or failure of the query alter, if requested the id
                of an insert, return the ID if was ok.
        """
        isOk = True
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                warnings.simplefilter("error", category=pymysql.Warning)
                with self.mysql_connection.cursor() as cursor:
                    try:
                        if returnId:
                            cursor.execute(query, params)
                            ultimo_id = cursor.lastrowid
                        else:
                            cursor.execute(query, params)
                    except pymysql.Warning as wing:
                        if 'Duplicate entry' not in wing.args[1]:
                            isOk = False
                        output_object = {
                            "Message": "Warning at SqlConnection class alter",
                            "Traceback": errorTrace()
                        }
                        if sendDatadogQuery:
                            output_object['Query': query]
                        print(errorTrace())
                        dg.log_datadog(output_object, 202, 'WARNING')
                if returnId:
                    return ultimo_id
        except Exception as err:
            output_object = {
                "Message": "Error at SqlConnection class alter",
                "Traceback": errorTrace()
            }
            if sendDatadogQuery:
                output_object['Query'] = query
            print(output_object)
            dg.log_datadog(output_object)
            isOk = False
        return isOk
