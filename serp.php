<?php
if(isset($_GET['search'])) {
   $keywords = explode(' ', $_GET['search']);
   $kw_str = "UPPER(k2.term) LIKE CONCAT('%', UPPER(:search0), '%')";
   $mesh_str = "UPPER(m2.term) LIKE CONCAT('%', UPPER(:search0), '%')";
   $ts_str = "to_tsquery('english', :terms)";
   $terms = "$keywords[0]";
   for($i=1; $i < count($keywords); $i++) {
      $kw_str = $kw_str . " OR UPPER(k2.term) LIKE CONCAT('%', UPPER(:search".$i."), '%')";
      $mesh_str = $mesh_str . " OR UPPER(m2.term) LIKE CONCAT('%', UPPER(:search".$i."), '%')";
      $terms = $terms . " | ".$keywords[$i];
   }

   $sql = "SELECT q.article_title FROM (".
                "SELECT pmid, article_title, ts FROM (".
                    "SELECT citation.pmid, article_title, term, ts FROM citation ".
                    "INNER JOIN cit_keyword ck on citation.pmid = ck.pmid ".
                    "INNER JOIN keyword k on ck.kwid = k.kwid) as k2 ".
                "WHERE $kw_str UNION ".
                "SELECT pmid, article_title, ts FROM (".
                    "SELECT citation.pmid, article_title, term, ts FROM citation ".
                    "INNER JOIN cit_mesh cm on citation.pmid = cm.pmid ".
                    "INNER JOIN mesh m on cm.meshid = m.meshid) as m2 ".
                "WHERE $mesh_str UNION ".
                "SELECT pmid, article_title, ts FROM citation ".
                "WHERE ts @@ $ts_str) as q ".
            "ORDER BY ts_rank(q.ts, $ts_str) DESC LIMIT 100;";

   $cred = parse_ini_file("../pg.ini");
   $conn = new PDO('pgsql:host=localhost;dbname=bstoneh1', $cred['user'], $cred['pswd']);
   $stmt = $conn->prepare($sql);
   for($i=0; $i < count($keywords); $i++) {
       $stmt->bindParam(":search".$i, $keywords[$i]);
   }
   $stmt->bindParam(":terms", $terms);
   $stmt->execute();
   $results = $stmt->fetchAll();
}?><!DOCTYPE HTML>
<html lang="en">
<head>
   <meta charset="UTF-8">
   <title>PubMed SERP</title>
   <style>
      table {
         border-collapse: collapse;
         width: 75%;
      }

      th, td {
         text-align: left;
      }

      tr:nth-child(even) {
         background-color: #f2f2f2;
      }
   </style>
</head>
<body>
   <form action="./serp.php" method="get">
      <span>
         <input type="text" id="search" name="search" value="<?=$_GET['search']?>">
         <input type="submit" name="submit" value="Search">
      </span>
   </form>
   <hr>
   <table>
      <?php
         echo "<tr><th>Article Title</th></tr>";
         foreach($results as $row) {
            echo "<tr><td>".$row['article_title']."</td></tr>\n";
      }?>
   </table>
</body>
</html>
