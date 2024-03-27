from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input, SwitchCase


class Ident(ProcessorBase):
    """
    Возвращает входное значение таким, каким оно было
    """
    name = 'ident'
    verbose_name = 'Identity'

    def process(self, num: float):
        return num


class SwitchStmt(ProcessorBase):
    """
    Вычисляет условие для switch
    """
    name = 'switch_stmt'
    verbose_name = 'Условие для switch'

    def process(self, num: Input(Ident)):
        if num < 0.0:
            return 'invert'
        return 'nested_switch'


class Invert(ProcessorBase):
    """
    Инвертирует число
    """
    name = 'invert'
    verbose_name = 'Инвертор'

    def process(self, num: Input(Ident)):
        return -num


class NestedSwitchStmt(ProcessorBase):
    """
    Вычисляет условие для switch
    """
    name = 'nested_switch_stmt'
    verbose_name = 'Условие для вложенного switch'

    def process(self, num: Input(Ident)):
        if num == 1.0:
            return 'double'
        return 'triple'


class DoubleNumber(ProcessorBase):
    """
    Умножает число на 2
    """
    name = 'double_number'
    verbose_name = 'Удвоение числа'

    def process(self, num: Input(Ident)):
        return num * 2


class TripleNumber(ProcessorBase):
    """
    Умножает число на 3
    """
    name = 'triple_number'
    verbose_name = 'Умножение числа на 3'

    def process(self, num: Input(Ident)):
        return num * 3


NestedSwitchCase = SwitchCase(
    name='nested_switch',
    switch=NestedSwitchStmt,
    cases=[
        ('double', DoubleNumber),
        ('triple', TripleNumber),
    ],
)


class Add3Node(ProcessorBase):
    """
    Прибавляет число 3
    """
    name = 'add_3'
    verbose_name = 'Прибавление числа 3'

    def process(self, num: NestedSwitchCase):
        return num + 3


SomeSwitchCase = SwitchCase(
    name='main_switch',
    switch=SwitchStmt,
    cases=[
        ('invert', Invert),
        ('nested_switch', Add3Node),
    ],
)


class Result(ProcessorBase):
    """
    Возвращает результат
    """
    name = 'result'
    verbose_name = 'Результат'

    def process(self, num: SomeSwitchCase, num2: Input(Ident)):
        return num + num2


sample_dag = build_dag(input_node=Ident, output_node=Result)
