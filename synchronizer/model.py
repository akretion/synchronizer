# -*- coding: utf-8 -*-
###############################################################################
#
#   Module for OpenERP
#   Copyright (C) 2014 Akretion (http://www.akretion.com).
#   @author Sébastien BEAU <sebastien.beau@akretion.com>
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU Affero General Public License as
#   published by the Free Software Foundation, either version 3 of the
#   License, or (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU Affero General Public License for more details.
#
#   You should have received a copy of the GNU Affero General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################


from openerp.osv import orm
import logging
_logger = logging.getLogger(__name__)


def _sync_get_ids(self, cr, uid, from_timekey, domain=None,
                  limit=None, to_timekey=None, context=None):
    if domain is None:
        domain = []
    self.check_access_rights(cr, uid or user, 'read')
    query = self._where_calc(cr, uid, domain, context=context)
    self._apply_ir_rules(cr, uid, query, 'read', context=context)
    from_clause, where_clause, where_clause_params = query.get_sql()

    #TODO add the support of inherits record
    if from_timekey:
        if where_clause:
            where_clause += ' AND'
        where_clause += \
            " (GREATEST(write_date, create_date) || '|' || id) > %s "
        where_clause_params.append(from_timekey)

    if to_timekey:
        if where_clause:
            where_clause += ' AND'
        where_clause += \
            " (GREATEST(write_date, create_date) || '|' || id) < %s "
        where_clause_params.append(to_timekey)

    where_str = where_clause and (" WHERE %s" % where_clause) or ''

    query_str = """
        SELECT
            id,
            GREATEST("%(table)s".write_date, "%(table)s".create_date)
                as update_time,
            GREATEST("%(table)s".write_date, "%(table)s".create_date)
                || '|' || id as timekey
        FROM """ % {'table': self._table} + from_clause + where_str + """
        ORDER BY timekey"""
    if limit:
        query_str += '\nLIMIT %d' % limit

    cr.execute(query_str, where_clause_params)
    results = cr.dictfetchall()
    if results:
        last = results.pop()
        ids = [r['id'] for r in results]
        ids.append(last['id'])
        return ids, last["timekey"]
    else:
        return [], from_timekey

def _prepare_sync_data(self, cr, uid, ids, key, context=None):
    if not hasattr(self, '_prepare_sync_data_%s' % key):
        _logger.error('The function _prepare_sync_data_%s do not exist', key)
        raise NotImplemented
    res = {}
    for record in self.browse(cr, uid, ids, context=context):
        res[record.id] = getattr(self, '_prepare_sync_data_%s' % key)(
            cr, uid, record, context=context)
    return res

def jsonify(record, depth=1):
    res = {}
    for field_name, field in record._columns.items():
        if field._type in ['char', 'boolean', 'datetime', 'float',
                           'integer', 'selection', 'text']:
            res[field_name] = record[field_name]
        elif depth > 0:
            if field._type == 'many2one':
                res[field_name] = jsonify(record[field_name], depth-1)
            elif field._type == 'one2many':
                data = []
                for item in record[field_name]:
                    data.append(jsonify(item))
                res[field_name] = data
    return res

def _prepare_sync_data_auto(self, cr, uid, record, context=None):
    return jsonify(record)

def get_sync_data(self, cr, uid, key, timekey, base_domain,
                  filter_domain, limit, context=None):
    ids, new_timekey = self._sync_get_ids(
        cr, uid, timekey,
        domain=base_domain + filter_domain,
        limit=limit,
        context=context)
    if timekey:
        all_ids, __ = self._sync_get_ids(
            cr, uid, timekey,
            domain=base_domain,
            to_timekey=new_timekey,
            context=context)
        remove_ids = list(set(all_ids).difference(set(ids)))
    else:
        remove_ids = []
    data = self._prepare_sync_data(cr, uid, ids, key, context=context)
    return {
        'data': data,
        'timekey': new_timekey,
        'remove_ids': remove_ids,
    }

orm.Model._sync_get_ids = _sync_get_ids
orm.Model._prepare_sync_data = _prepare_sync_data
orm.Model.get_sync_data = get_sync_data
orm.Model._prepare_sync_data_auto = _prepare_sync_data_auto
