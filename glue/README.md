# Desenvolvendo Glue Jobs

## Docker 

Usando as imagens `glue/aws-glue-libs` do [Amazon ECR Public Gallery](https://gallery.ecr.aws/glue/aws-glue-libs) podemos obter um ambiente local de desenvolvimento próximo ao utilizado nas execuções dos jobs Glue, podendo simular inicialização, importações, comportamento de logging, entre outros aspectos dos jobs Glue localmente, sem custos e com maior agilidade para testes - incluindo um comportammento similar de interação com a AWS, evitando adaptações para executar o código locamente e no ambiente cloud.

Para execução de uma sessão interativa de podemos usar o seguinte comando:
```sh
docker run -it --rm  -v ~/.aws:/home/hadoop/.aws \
    -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ \
    -e AWS_PROFILE=$PROFILE_NAME \
    --name glue5_spark_submit public.ecr.aws/glue/aws-glue-libs:5 \
    pyspark
```

Para executar um script podemos usar o seguinte comando:
```sh
docker run -it --rm  -v ~/.aws:/home/hadoop/.aws \
    -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ \
    -e AWS_PROFILE=$PROFILE_NAME \
    --name glue5_spark_submit public.ecr.aws/glue/aws-glue-libs:5  
    \ spark-submit /home/hadoop/workspace/glue_hello_glue.py
```
### Requimentos

- Docker for MacOS and Linux must be installed, Docker for Desktop must be installed on Windows
- AWS Cli must be installed
- Python3 and Pip3 must be instaled