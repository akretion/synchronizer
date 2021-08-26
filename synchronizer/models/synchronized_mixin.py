# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl.html).
from odoo import models, api, fields
from datetime import datetime
import logging

_logger = logging.getLogger(__name__)


def jsonify(record, depth=1):
    res = {}
    for field_name, field in record._fields.items():
        if field.type in [
            "char",
            "boolean",
            "datetime",
            "float",
            "integer",
            "selection",
            "text",
        ]:
            res[field_name] = record[field_name]
        elif depth > 0:
            if field.type == "many2one":
                res[field_name] = jsonify(record[field_name], depth - 1)
            elif field.type == "one2many":
                data = []
                for item in record[field_name]:
                    data.append(jsonify(item))
                res[field_name] = data
    return res


class SynchronizedMixin(models.AbstractModel):
    _name = "synchronized.mixin"

    timekey = fields.Char(index=True)

    _sql_contraint = {("timekey_uniq", "unique(timekey)", "Timekey must be uniq")}

    def _init_timekey(self):
        records = self.search([], order="write_date asc")
        records._update_timekey()

    def _update_timekey(self):
        for record in self:
            timekey = datetime.now().strftime("%s%f")
            self.env.cr.execute(
                "UPDATE " + self._table + " SET timekey = %s WHERE id = %s",
                (timekey, record.id),
            )

    def write(self, vals):
        res = super().write(vals)
        self._update_timekey()
        return res

    @api.model_create_multi
    def create(self, vals_list):
        records = super().create(vals_list)
        records._update_timekey()
        return records

    def _sync_get_ids(self, from_timekey, domain=None, limit=None, to_timekey=None):
        if domain is None:
            domain = []
        self.check_access_rights("read")
        query = self._where_calc(domain)
        self._apply_ir_rules(query, "read")
        from_clause, where_clause, where_clause_params = query.get_sql()

        # TODO add the support of inherits record
        if from_timekey:
            if where_clause:
                where_clause += " AND"
            where_clause += " timekey > %s "
            where_clause_params.append(from_timekey)

        if to_timekey:
            if where_clause:
                where_clause += " AND"
            where_clause += " timekey < %s "
            where_clause_params.append(to_timekey)

        where_str = where_clause and (" WHERE %s" % where_clause) or ""

        query_str = (
            """
            SELECT
                id,
                timekey
            FROM """
            % {"table": self._table}
            + from_clause
            + where_str
            + """
            ORDER BY timekey"""
        )

        self.env.cr.execute(query_str, where_clause_params)
        results = self.env.cr.dictfetchall()
        # Avoid using limit in query as in some case it may confuse postgresql query
        # planner. Since we order by timekey, it would try to use the timekey index
        # even if it is not even used in where clause. Then the query may take a
        # really long time in case there are a lot of rows.
        # Avoiding the limit, it will always use the where clause relative indexes
        if limit:
            results = results[:limit]
        if results:
            last = results.pop()
            ids = [r["id"] for r in results]
            ids.append(last["id"])
            return ids, last["timekey"]
        else:
            return [], from_timekey

    def _prepare_sync_data(self, key):
        if not hasattr(self, "_prepare_sync_data_%s" % key):
            _logger.error("The function _prepare_sync_data_%s do not exist", key)
            raise NotImplementedError
        res = {}
        for record in self:
            res[record.id] = getattr(record, "_prepare_sync_data_%s" % key)()
        return res

    def _prepare_sync_data_auto(self):
        self.ensure_one()
        return jsonify(self)

    def get_sync_data(self, key, timekey, base_domain, filter_domain, limit, current_ids):
        ids, new_timekey = self._sync_get_ids(
            timekey, domain=base_domain + filter_domain, limit=limit
        )
        if timekey and current_ids:
             # to get ids to remove, get valid ids in the current external apps list
             # (current_list) and then remove the one not valid anymore
             filter_domain = filter_domain + [("id", "in", current_ids)]
             current_valid_ids, _dummy_timekey = self._sync_get_ids(
                     None, domain=base_domain + filter_domain)
             remove_ids = list(set(current_ids).difference(current_valid_ids))
        else:
            remove_ids = []
        data = self.browse(ids)._prepare_sync_data(key)
        return {
            "data": data,
            "timekey": new_timekey,
            "remove_ids": remove_ids,
        }
