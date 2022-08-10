#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback

from mods import datadog as dg


def to_answers_integration(conv_response: dict) -> (dict, bool, bool):
    context = {}
    new_watson_call = False
    it_has_integration = False
    try:
        pass
    except Exception as unknown_exception:
        datadog_data = {
            'Message': f'Unknown exception at to_answers_integration: {unknown_exception}',
            'Traceback': traceback.format_exc()
        }
        dg.log_datadog(datadog_data)
    return context, new_watson_call, it_has_integration
