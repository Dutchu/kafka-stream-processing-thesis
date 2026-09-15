# Frontend — wskazówka

UI mieszka w `../dashboard-front/` (osobny projekt widoku: `README.md` tam).
Ten plik istnieje tylko po to, żeby nikt nie szukał widoku w `src/main/resources/`
— jar nie zawiera już statyków; Javalin serwuje `/opt/thesis-frontend/`
(deployowane kanałem frontendu).
