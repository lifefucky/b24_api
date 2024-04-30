import asyncio
import logging

import pandas as pandas

import requests
import time
import traceback

from urllib.parse import quote_plus


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class Bitrix:
    def __init__(self, hostname, token_for_add = '', token_for_list = ''):
        self.__hostname = hostname
        self.__token_for_add = token_for_add
        self.__token_for_list = token_for_list
        self.bx_page_size = 50
        self.bx_batch_commands = 50

    def _call_bx_method(self, method, data):
        url = (
            f"https://{self.__hostname}/rest/1/{self.__token_for_list}"
        )
        def _call_bx(method, data):
            return requests.post(
                url='{}/{}/'.format(url, method),
                data=data
            )

        response = None
        call_delay = 1
        while call_delay < 3:
            logging.info(call_delay)
            try:
                response = _call_bx(method, data)
                if response.ok and response.json():
                    return response
                else:
                    logging.info(response.status_code)
                    logging.info(response.content)
            except:
                tb = traceback.format_exc()
                logging.info(tb)
                pass
            time.sleep(call_delay)
            call_delay += call_delay
        return response

    async def item_list_batch(self, deal_ids, fields, page_idx=0):
        data = []
        data.append('halt=0')

        max_len_id = (self.bx_page_size - 1) * self.bx_batch_commands
        len_deal_ids = len(deal_ids) if len(deal_ids) < max_len_id else max_len_id

        logging.info(f"{len_deal_ids} items will be upload;")
        deal_ids_parts = [deal_ids[i:i + 50] for i in range(0, len_deal_ids, 50)]
        for deal_ids_part in deal_ids_parts:
            cmd = 'cmd[page_{}]=crm.deal.list'.format(page_idx)
            cmd_args = []
            for idx, field in enumerate(fields):
                cmd_args.append('select[{}]={}'.format(idx, field))
            for idx, deal_id in enumerate(deal_ids_part):
                cmd_args.append('filter[ID][{}]={}'.format(idx, deal_id))
            cmd_args = quote_plus('?' + '&'.join(cmd_args))
            cmd_args += quote_plus('&start=-1')
            data.append(cmd + cmd_args)
            page_idx += 1

        data = '&'.join(data)
        logging.info(data)
        response = self._call_bx_method('batch', data)
        response_data = response.json()
        items = []
        for cmd_key, cmd_result in response_data.get('result', {}).get('result', {}).items():
            for item in cmd_result:
                if not next((_ for _ in items if _['ID'] == item['ID']), None):
                    items.append(item)
        return items

    def fetch_stages_df(self, deal_ids, fields):
        df = pandas.DataFrame()

        if deal_ids:
            items = asyncio.run(
                self.item_list_batch(deal_ids=deal_ids, fields=fields, page_idx=0)
            )

            logging.info(f"{len(items)}/{len(deal_ids)} items info received;")

            unsynced_items_ids = [el for el in deal_ids if not any(str(el) == item['ID'] for item in items)]
            if unsynced_items_ids:
                logging.info(f"Got unsynced items: {unsynced_items_ids}")

            force_int_fields = (
                'ID'
            )

            for item in items:
                for field, value in item.items():
                    if field in force_int_fields:
                        try:
                            item[field] = int(value)
                        except (TypeError, ValueError):
                            item[field] = None

            src_table_schema = []
            for field in fields:
                if field in force_int_fields:
                    src_table_schema.append({'name': field, 'type': 'INTEGER'})
                else:
                    src_table_schema.append({'name': field, 'type': 'STRING'})

            df = pandas.json_normalize(items)
            logging.info(f"{df.shape[0]} to push;")
        else:
            logging.info("Empty deals item inserted.")
        return df


hostname = ''
token_for_list = ''
bx = Bitrix(hostname=hostname, token_for_list=token_for_list)
bx.fetch_stages_df(deal_ids=[2, 1000, 10000], fields=['ID', 'STAGE_ID'])
