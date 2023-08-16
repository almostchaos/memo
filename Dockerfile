# -- BUILD STAGE --
# We use a non-Alpine container to do the build because Alpine is not
# supported as an official distribution for the AWS CLI and getting it
# on there is mind-numbingly painful.  So we use a standard, supported
# Linux distro for the build stage and then copy the results to the Alpine
# image when it's done.
FROM clojure:tools-deps

RUN mkdir /service
WORKDIR /service
COPY deps.edn .
COPY src ./src
EXPOSE 8080
CMD [ "clojure", "-M", "-m", "memo.service"]