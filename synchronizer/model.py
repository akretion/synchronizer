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

from openerp import models, api, fields
from datetime import datetime
import logging
_logger = logging.getLogger(__name__)


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


class SynchronizedMixin(models.AbstractModel):
    _name = 'synchronized.mixin'

    timekey = fields.Char(index=True)

    _sql_contraint = {
        ('timekey_uniq', 'unique(timekey)', 'Timekey must be uniq')
    }

    @api.model
    def _init_timekey(self):
        records = self.search([], order='write_date asc')
        records._update_timekey()

    @api.multi
    def _update_timekey(self):
        for record in self:
            timekey = datetime.now().strftime('%s%f')
            self.env.cr.execute(
                "UPDATE " +
                self._table +
                " SET timekey = %s WHERE id = %s",
                (timekey, record.id))

    @api.multi
    def write(self, vals):
        res = super(SynchronizedMixin, self).write(vals)
        self._update_timekey()
        return res

    @api.model
    def create(self, vals):
        record = super(SynchronizedMixin, self).create(vals)
        record._update_timekey()
        return record

    @api.model
    def _sync_get_ids(self, from_timekey, domain=None, limit=None,
                      to_timekey=None):
        if domain is None:
            domain = []
        self.check_access_rights('read')
        query = self._where_calc(domain)
        self._apply_ir_rules(query, 'read')
        from_clause, where_clause, where_clause_params = query.get_sql()

        # TODO add the support of inherits record
        if from_timekey:
            if where_clause:
                where_clause += ' AND'
            where_clause += \
                " timekey > %s "
            where_clause_params.append(from_timekey)

        if to_timekey:
            if where_clause:
                where_clause += ' AND'
            where_clause += \
                " timekey < %s "
            where_clause_params.append(to_timekey)

        where_str = where_clause and (" WHERE %s" % where_clause) or ''

        query_str = """
            SELECT
                id,
                timekey
            FROM """ % {'table': self._table} + from_clause + where_str + """
            ORDER BY timekey"""
        if limit:
            query_str += '\nLIMIT %d' % limit

        self.env.cr.execute(query_str, where_clause_params)
        results = self.env.cr.dictfetchall()
        if results:
            last = results.pop()
            ids = [r['id'] for r in results]
            ids.append(last['id'])
            return ids, last["timekey"]
        else:
            return [], from_timekey

    @api.multi
    def _prepare_sync_data(self, key):
        if not hasattr(self, '_prepare_sync_data_%s' % key):
            _logger.error('The function _prepare_sync_data_%s do not exist',
                          key)
            raise NotImplemented
        res = {}
        for record in self:
            res[record.id] = getattr(self, '_prepare_sync_data_%s' % key)()
        return res

    @api.multi
    def _prepare_sync_data_auto(self):
        self.ensure_one()
        return jsonify(self)

    @api.model
    def get_sync_data(self, key, timekey, base_domain,
                      filter_domain, limit):
        ids, new_timekey = self._sync_get_ids(
            timekey, domain=base_domain + filter_domain, limit=limit)
        if timekey:
            if timekey == new_timekey and len(ids) < limit:
                new_timekey = None
            all_ids, delete_timekey = self._sync_get_ids(
                timekey, domain=base_domain, to_timekey=new_timekey)
            remove_ids = list(set(all_ids).difference(set(ids)))
            if not new_timekey and delete_timekey:
                new_timekey = delete_timekey
        else:
            remove_ids = []
        data = self.browse(ids)._prepare_sync_data(key)
        return {
            'data': data,
            'timekey': new_timekey,
            'remove_ids': remove_ids,
        }
