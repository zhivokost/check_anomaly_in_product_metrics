# Система алертов основных метрик приложения
Настроена автоматическая проверка состояния ключевых метрик с периодичностью каждые 15 минут. В случае обнаружения аномального значения, в чат отправляется алерт.
Реализованы следующие функции:
1. Выгрузка из БД метрик новостной ленты;
2. Выгрузка из БД метрик мессенджера;
3. Расчет нижней и верхней границы доверительного интервала для рассматриваемой метрики на основе средних значений предыдущих дней;
4. Расчет нижней и верхней границы доверительного интервала для рассматриваемой метрики на основе межквартильного размаха;
5. Расчет нижней и верхней границы доверительного интервала для рассматриваемой метрики на основе прогнозирования временного ряда методом тройного экспоненциального сглаживания, или модели Хольта-Винтерса (Holt-Winters);
6. Формирование текста, построение визуализации и их отправка в Telegram-чат.
Автоматизация настроена с помощью Airflow.

### Инструменты: pandahouse, pandas, matplotlib, seaborn, telegram, airflow
