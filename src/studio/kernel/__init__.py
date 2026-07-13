"""M2 内核:事件溯源工作流的通用、领域无关的机制。

依赖方向:application/infrastructure/examples 依赖 kernel;kernel 不依赖它们。
kernel 泛型化,不 import 任何具体应用的 payload 联合。
"""
