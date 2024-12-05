# PAPA EVENTS - WIP

Libreria para facilitar la comunicación entre servicios con interfaz declarativa

### Inicialización

La libreria necesita correr dentro de un event loop, se puede integrar con cualquier app que tenga uno o crear uno para correr standalone

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from papa_events import PapaApp


event_app = PapaApp(broker_uri="amqp://test:test@127.0.0.1/")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await event_app.start()
    yield
    await event_app.stop()

    
app = FastAPI(lifespan=lifespan)
```

```python
# Todo ejemplo event loop standalone
```

### Consumo de eventos

Se decora la funcion que hará de controller para el evento, este tendrá un único parametro tipado con una subclase de pydantic.BaseModel
```python
from pydantic import BaseModel


class NewClient(BaseModel):
    name: str
    age: int


# Consumo de 1 solo evento
@event_app.on_event(['user.created'], use_case_name="send_welcome_email_on_user_created")
async def email_sender(event: NewClient):
   """
   do stuff
   """

# Consumo de varios eventos
@event_app.on_event(['user.created, company.deleted'], use_case_name="update_bank_account_on_xxxx")
async def update_bank_account(event: NewClient):
   """
   do stuff
   """

# Consumo de varios eventos con wildcards
@event_app.on_event(['user.*'], use_case_name="user_log_on_user_event")
async def user_log(event: NewClient):
   """
   do stuff
   """
``` 

### Publicación de eventos

- De forma aislada

```python
from pydantic import BaseModel


class NewClient(BaseModel):
    name: str
    age: int

event = NewClient(name="carlos", age=21)
papa_app.new_event("user.created", event)
```

- Como resultado del procesamiento de un evento

Se manda automaticamente uno o varios eventos si el valor de retorno es una lista de diccionarios

```python
from pydantic import BaseModel


class NewClient(BaseModel):
    name: str
    age: int

@event_app.on_event(['user.created'], use_case_name="send_email_on_user_created")
async def new_client(event: NewClient) -> list[dict]:
    """
   do stuff
   """
    return [{
        "name": "email.sended", # Event name
        "payload": # Event payload
            {
                "destination": "hola@test.com", 
                "body": "bienvenido"
            }
    }]
```

### Flujo

El sistema genera tantas colas como casos de usos requieran los consumidores. El broker dirije automaticamente a tantas colas como sea necesario el evento.

- Si falla el procesado del evento se manda a una cola de retry, donde espera 10 segundos y vuelve a encolar el mensaje
- Si el numero de reintentos supera el campo retries del caso de uso se manda a una cola dlq donde permanecera hasta intervención manual.

![flow](/docs/img/flow.png)

