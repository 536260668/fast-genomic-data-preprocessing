//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) : rows(r), cols(c) { linear = new T[r * c]; }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() { delete[] linear; }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    data_ = new T *[r];
    for (int i = 0; i < r; i++) {
      data_[i] = Matrix<T>::linear + i * c;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return Matrix<T>::rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return Matrix<T>::cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override {
    if (i >= 0 && i < Matrix<T>::rows && j >= 0 && j < Matrix<T>::cols) {
      return data_[i][j];
    }
    return data_[0][0];
  }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override {
    if (i >= 0 && i < Matrix<T>::rows && j >= 0 && j < Matrix<T>::cols) {
      data_[i][j] = val;
    }
  }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    for (int i = 0; i < GetRows(); i++) {
      for (int j = 0; j < GetColumns(); j++) {
        SetElem(i, j, arr[i * GetColumns() + j]);
      }
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override { delete[] data_; }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetRows() != mat2->GetRows() || mat1->GetColumns() != mat2->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(mat1->GetRows(), mat2->GetRows()));
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat2->GetColumns(); j++) {
        ret->SetElem(i, j, mat1->GetElem(i, j) + mat2->GetElem(i, j));
      }
    }
    return ret;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    // TODO(P0): Add code
    if (mat1->GetColumns() != mat2->GetRows()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> ret(new RowMatrix<T>(mat1->GetRows(), mat2->GetColumns()));
    for (int i = 0; i < mat1->GetRows(); i++) {
      for (int j = 0; j < mat2->GetColumns(); j++) {
        T tmp = mat1->GetElem(i, 0) * mat2->GetElem(0, j);
        for (int k = 1; k < mat1->GetColumns(); k++) {
          tmp += mat1->GetElem(i, k) * mat2->GetElem(k, j);
        }
        ret->SetElem(i, j, tmp);
      }
    }
    return ret;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    // TODO(P0): Add code
    if (matA->GetColumns() != matB->GetRows() || matA->GetRows() != matC->GetRows() ||
        matB->GetColumns() != matC->GetColumns()) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    return AddMatrices(MultiplyMatrices(std::move(matA), std::move(matB)), std::move(matC));
  }
};
}  // namespace bustub
