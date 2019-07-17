package in.nimbo.dao.elastic;

import in.nimbo.exception.ElasticException;

import java.util.List;

public interface ElasticDAO {
    void save(String link, String text) throws ElasticException;

    String get(String link) throws ElasticException;

    List<String> getAllLinks() throws ElasticException;
}
