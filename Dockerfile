FROM jupyter/all-spark-notebook

LABEL maintainer="Abdullah McDonald <mcdonaldabdullah@gmail.com> "

WORKDIR /shared_folder

COPY . /shared_folder

RUN pip --no-cache-dir install numpy pandas matplotlib jupyter tweepy autocorrect profanity_check

EXPOSE 8888

VOLUME /home/Downloads/Twitter

CMD ["jupyter", "notebook", "--ip='*'", "--port=8888", "--no-browser", "--allow-root"] 
