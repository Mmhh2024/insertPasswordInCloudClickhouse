package fr.project;


import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

@SpringBootApplication
public class InsertpasswordcloudApplication {

    @Value("${app.input-directory}")
    private String inputDirectory;

    @Value("${app.output-directory}")
    private String outputDirectory;

    @Value("${app.db.url}")
    private String dbUrl;

    @Value("${app.db.user}")
    private String dbUser;

    @Value("${app.db.password}")
    private String dbPassword;

    private final Logger logger = LoggerFactory.getLogger(InsertpasswordcloudApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(InsertpasswordcloudApplication.class, args);
    }

    @Bean
    public CommandLineRunner run(ResourceLoader resourceLoader) {
        return args -> {
            File repertoirein = new File(inputDirectory);
            File repertoireout = new File(outputDirectory);

            if (repertoirein.isDirectory()) {
                logger.info("Répertoire");

                int batchIndex = 0;
                File[] listOfFiles = repertoirein.listFiles();
                if (listOfFiles.length > 0) {
                    try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
                        connection.setAutoCommit(false);

                        for (File file : listOfFiles) {
                            batchIndex = 0;
                            logger.info("Traitement du fichier " + file.getName());

                            String fileName = inputDirectory + "/" + file.getName();
                            String outputFileName = outputDirectory + "/" + file.getName() + ".ok";

                            try (Scanner scanner = new Scanner(file);
                                 PreparedStatement statement = connection.prepareStatement("INSERT INTO passwordtbl VALUES (?)")) {
                                while (scanner.hasNextLine()) {
                                    String buffer = scanner.nextLine();
                                    batchIndex++;

                                    String[] bufferTab = buffer.split(":");
                                    if (bufferTab.length > 0) {
                                        statement.setString(1, bufferTab[0]);
                                        statement.addBatch();

                                        if (batchIndex == 100_000) {
                                            statement.executeBatch();
                                            connection.commit();
                                            batchIndex = -1;
                                        }
                                    }
                                }
                                statement.executeBatch();
                                connection.commit();
                                scanner.close();
                                file.renameTo(new File(outputFileName));
                            }
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        logger.error("Impossible de se connecter ...", ex);
                    }
                } else {
                    logger.info("Répertoire vide ...");
                }
            }
        };
    }
}
