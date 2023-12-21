from IPython.core.magic import Magics, cell_magic, magics_class


@magics_class
class OpenArkMagic(Magics):
    @cell_magic
    def sql(self, line: str, cell: str) -> None:
        # parse args
        style = line

        # flatten cell
        query = ''
        for token in cell.replace('\n', ' ').split(' '):
            token = token.strip()
            if len(token) == 0:
                continue
            query += f'{token} '

        self.shell.run_cell(
            f'OpenArk.get_global_instance().get_global_namespace().sql_and_draw({query = }, {style =})',
        )
