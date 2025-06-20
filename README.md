
# (Partial) HyFD

An extension of the **HyFD** algorithm for efficient discovery of functional dependencies in relational data. This project is heavily inspired by and based on the original work.

## About The Project

Functional Dependencies (FDs) are a fundamental concept in database theory, expressing constraints where the values in one set of columns uniquely determine the values in another column (e.g., `zip_code -> city`). Discovering these FDs from raw data is a critical task for data cleaning, schema normalization, and data exploration.

This project provides a partial version of **HyFD**.

## Acknowledgments

If you use the concepts or code from this project in your research, please cite the original HyFD paper and acknowledge the original repository.

*   **Original Paper:** Thorsten Papenbrock, Felix Naumann, "A Hybrid Approach to Functional Dependency Discovery". *Proceedings of the 2016 International Conference on Management of Data (SIGMOD '16)*.
    *   **PDF Link:** [https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2016/mod922.pdf](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2016/mod922.pdf)

*   **Original Metanome Repository:** The original Java implementation is available as part of the Metanome data profiling framework.
    *   **GitHub Link:** [https://github.com/HPI-Information-Systems/metanome-algorithms](https://github.com/HPI-Information-Systems/metanome-algorithms)

### BibTeX

```bibtex
@inproceedings{papenbrock2016hybrid,
  title={A hybrid approach to functional dependency discovery},
  author={Papenbrock, Thorsten and Naumann, Felix},
  booktitle={Proceedings of the 2016 International Conference on Management of Data},
  pages={821--833},
  year={2016}
}
```
