package com.neo.service;

import com.neo.model.dto.RequestDTO;
import com.neo.model.entity.HandUp;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface FileProcessService {
    int ImportTxtForNormal(File txtFile) throws SQLException;

    String ImportTxtForBigFile(RequestDTO requestDTO) throws ExecutionException, InterruptedException;

    int insetBetch(List<HandUp> list);
}
