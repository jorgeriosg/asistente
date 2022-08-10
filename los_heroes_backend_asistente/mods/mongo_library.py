#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mongo connection module
"""
import pymongo
from mods.config import MONGO_URI, DATA_BASE, INTERACTIONS_COLLECTION, BENEFICIOS_COLLECTION, VALORACION_COLLECTION


def mongo_connect(collection=None):
    """
    bd connection
    :return:
    """
    col = INTERACTIONS_COLLECTION
    mongo = pymongo.MongoClient(MONGO_URI)
    dbase = mongo[DATA_BASE]
    interaction = dbase[col] if collection is None else dbase[collection]
    return interaction


def single_find(query, sort=False):
    """
    bd query
    :param cid:
    :param type:
    :return:
    """
    interaction = mongo_connect()
    if sort:
        last_interaction = interaction.find(query).sort('datetime', 1)
        if last_interaction != []:
            last_interaction = list(last_interaction)
        return last_interaction

    last_interaction = list(interaction.find(query, {'_id': False}).sort('datetime', -1).limit(1))
    if last_interaction != []:
        last_interaction = last_interaction[0]
    return last_interaction


def find(find, aggregate=None, filtro=None, collection=None):
    """
    :param find:
    :param aggregate:
    :param filtro:
    :return:
    """
    if collection == 'beneficios':
        collection = BENEFICIOS_COLLECTION
    elif collection == 'valoracion':
        collection = VALORACION_COLLECTION

    connection = mongo_connect(collection)
    if aggregate:
        resp = connection.aggregate(find)
    else:
        resp = connection.find(find, filtro) if filtro else connection.find(find)
    return list(resp)


def insert(query, collection: str = None):
    """
    insert query
    :param query:
    :return:
    """
    if collection == 'beneficios':
        collection = BENEFICIOS_COLLECTION
    elif collection == 'valoracion':
        collection = VALORACION_COLLECTION

    connection = mongo_connect(collection)
    return connection.insert_one(query)
