# zooniverse-db

### Running Server
Start the backend server using the command:
'poetry run maestro start -f'

### Accessing Route Endpoints
To access or test any route endpoints, you need the name of the backend which listed when maestro starts up or is found im backend.yml. The backend name along with swagger needs to be apart of base url to view the toures endpoints which are controlled by Swagger.

Syntax:
>27.0.0.1:8000/zooniverse-db/swagger/

### Adding dependencies 
There is a bug in poetry when installing packages without ssh key password input automatically setup. Added '-vvv' at the end of commands.